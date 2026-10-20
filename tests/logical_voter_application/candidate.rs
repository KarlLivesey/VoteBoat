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
    assert_eq!(
        a.deployment_requirements().unwrap().application_schema,
        LOGICAL_ENROLLMENT_APPLICATION_SCHEMA
    );
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
    assert_eq!(
        other.restore_checkpoint(LOGICAL_ENROLLMENT_APPLICATION_SCHEMA, 0, &bytes),
        Err(ApplicationError::InvalidCheckpoint)
    );
    assert_eq!(other.checkpoint(usize::MAX).unwrap(), before);
    let legacy = app(LogicalLimits::default())
        .checkpoint(usize::MAX)
        .unwrap();
    assert_eq!(
        other.restore_checkpoint(LOGICAL_APPLICATION_SCHEMA, 0, &legacy),
        Err(ApplicationError::UnsupportedSchema)
    );
    assert_eq!(other.checkpoint(usize::MAX).unwrap(), before);
    let mut restored = construct(plan(), LogicalLimits::default()).unwrap();
    restored
        .restore_checkpoint(LOGICAL_ENROLLMENT_APPLICATION_SCHEMA, 0, &bytes)
        .unwrap();
    assert_eq!(restored.checkpoint(usize::MAX).unwrap(), bytes);
}

#[test]
fn readiness_compares_every_limit_mapping_plan_and_control_profile() {
    // DR1 round 6 audit: the logical voter declared its family alone, so a
    // learner configured with other action limits, parent log limits, mapping,
    // enrollment plan or control profile got a readiness receipt.
    use voteboat::raft::ReadinessRequirements;
    let limits = LogicalLimits::default();
    let base = app(limits);
    let declared = base.deployment_requirements().unwrap();
    assert_eq!(
        declared.identity.configuration,
        [
            limits.max_actions as u64,
            limits.max_retained_action_bytes as u64,
            limits.control_actions as u64,
            limits.control_action_bytes as u64,
        ]
    );
    // Mapping 10, limits 16, codec versions 3, wire limits 9, two flags.
    assert_eq!(
        declared.inner_profile.plan().segment(0).unwrap().len(),
        10 + 16 + 3 + 9 + 1 + 1
    );
    let covering = |r: ReadinessRequirements| ReadinessRequirements {
        command_bytes: r.command_bytes.max(declared.command_bytes),
        snapshot_bytes: r.snapshot_bytes.max(declared.snapshot_bytes),
        ..r
    };
    // Another child group: the mapping and the child bootstrap agree.
    let mapped = |child: u128| {
        LogicalApplication::new(
            LogicalStoreMapping::new(group(1), NodeId::new(1).unwrap(), store(1), group(child))
                .unwrap(),
            bootstrap(group(child), 2, store(2)),
            bootstrap(group(1), 1, store(1)),
            limits,
            NativeLogCodec,
            NativeSnapshotCodec,
            NativeWireCodec::with_membership(WireLimits::default()).unwrap(),
        )
        .unwrap()
    };
    let wrapper = |mappings: Vec<LogicalStoreMapping>| {
        LogicalActivationApplication::new(
            group(1),
            mappings,
            LogicalActivationLimits {
                command_bytes: 4096,
                ..LogicalActivationLimits::default()
            },
            Counter::new(16).unwrap(),
        )
        .unwrap()
        .control_profile()
    };
    let profiled = base
        .clone()
        .with_activation_control_profile(wrapper(vec![mapping()]))
        .unwrap();
    let others = [
        app(LogicalLimits {
            max_actions: limits.max_actions - 1,
            ..limits
        }),
        app(LogicalLimits {
            max_capsule_bytes: limits.max_capsule_bytes - 1,
            ..limits
        }),
        app(LogicalLimits {
            parent_log: LogLimits {
                max_entries_per_group: limits.parent_log.max_entries_per_group + 1,
                ..limits.parent_log
            },
            ..limits
        }),
        mapped(3),
        profiled.clone(),
        construct(plan(), limits).unwrap(),
    ];
    for other in &others {
        let theirs = other.deployment_requirements().unwrap();
        assert_eq!(other.validate_deployment_requirements(theirs), Ok(()));
        assert_eq!(
            base.validate_deployment_requirements(covering(theirs)),
            Err(ApplicationError::UnsupportedSchema),
            "{theirs:?}"
        );
        assert_eq!(
            other.validate_deployment_requirements(covering(declared)),
            Err(ApplicationError::UnsupportedSchema)
        );
    }
}

#[test]
fn the_largest_logical_voter_plan_fits_the_deployment_plan_bound() {
    let limits = LogicalLimits::default();
    let wrapper = |mappings: Vec<LogicalStoreMapping>| {
        LogicalActivationApplication::new(
            group(1),
            mappings,
            LogicalActivationLimits {
                command_bytes: 4096,
                ..LogicalActivationLimits::default()
            },
            Counter::new(16).unwrap(),
        )
        .unwrap()
        .control_profile()
    };
    // The largest plan: a candidate with a 16-mapping control profile that
    // includes the candidate's own mapping (voter 3 to child group 2).
    let full = wrapper(
        (1..=16)
            .map(|n| {
                let child = if n == 3 { 2 } else { 100 + n as u128 };
                LogicalStoreMapping::new(
                    group(1),
                    NodeId::new(n).unwrap(),
                    store(n as u128),
                    group(child),
                )
                .unwrap()
            })
            .collect(),
    );
    let largest = construct(plan(), limits)
        .unwrap()
        .with_activation_control_profile(full)
        .unwrap()
        .deployment_requirements()
        .unwrap();
    assert!(largest.inner_profile.layers().is_some());
    assert_eq!(largest.inner_profile.plan().words().len(), 1 + 173);
    assert!(
        largest.inner_profile.plan().words().len() <= voteboat::raft::MAX_DEPLOYMENT_PLAN_WORDS
    );
}
