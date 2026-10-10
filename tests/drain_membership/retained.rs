// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

fn retained(g: u128) -> DrainRetainedGroup {
    DrainRetainedGroup {
        group: group(g),
        original: configuration(3, &[2, 3], &[1]),
    }
}
fn mixed(retained: Vec<DrainRetainedGroup>) -> Result<MembershipDrainPlan, DrainPlanError> {
    MembershipDrainPlan::with_retained_learners(
        owner(),
        OperationId::new(101).unwrap(),
        vec![entry(2)],
        retained,
    )
}
#[test]
fn complete_inventory_binds_disjoint_roles_and_preserves_voter_only_digest() {
    let original = plan(vec![entry(2)]);
    assert_eq!(mixed(vec![]).unwrap().digest(), original.digest());
    let combined = mixed(vec![retained(1), retained(3)]).unwrap();
    assert_eq!(combined.groups(), original.groups());
    assert_eq!(combined.retained_learners().len(), 2);
    assert_eq!(
        combined
            .assignments()
            .iter()
            .map(|e| e.group)
            .collect::<Vec<_>>(),
        vec![group(1), group(2), group(3)]
    );
    let record = combined.record(1).unwrap();
    assert_eq!(record.request.groups, combined.assignments());
    let journal = Journal(record);
    assert_eq!(original.verify(&journal), Err(DrainPlanError::WrongIntent));
    let mut changed = retained(1);
    changed.original = configuration(4, &[2, 3], &[1]);
    assert_eq!(
        mixed(vec![changed, retained(3)]).unwrap().verify(&journal),
        Err(DrainPlanError::WrongIntent)
    );
    for list in [
        vec![retained(2)],
        vec![retained(1), retained(1)],
        vec![retained(3), retained(1)],
    ] {
        assert_eq!(mixed(list), Err(DrainPlanError::InvalidPlan));
    }
}
#[test]
fn retained_assignments_require_exact_learner_identity_and_combined_bounds() {
    for original in [
        configuration(3, &[1, 2, 3], &[]),
        configuration(3, &[2, 3], &[]),
    ] {
        assert_eq!(
            mixed(vec![DrainRetainedGroup {
                group: group(1),
                original
            }]),
            Err(DrainPlanError::InvalidPlan)
        );
    }
    let wrong_store = PeerIdentity {
        node: owner().node,
        store: identity(99),
    };
    assert_eq!(
        MembershipDrainPlan::with_retained_learners(
            wrong_store,
            OperationId::new(101).unwrap(),
            vec![],
            vec![retained(1)]
        ),
        Err(DrainPlanError::InvalidPlan)
    );
    let only = MembershipDrainPlan::with_retained_learners(
        owner(),
        OperationId::new(101).unwrap(),
        vec![],
        vec![retained(1)],
    )
    .unwrap();
    assert!(only.groups().is_empty());
    assert_eq!(only.assignments().len(), 1);
    let oversized = (3..=1026).map(retained).collect();
    assert_eq!(mixed(oversized), Err(DrainPlanError::InvalidPlan));
    let mut excess_capacity =
        Vec::with_capacity(MAX_DRAIN_PLAN_BYTES / std::mem::size_of::<DrainRetainedGroup>() + 1);
    excess_capacity.push(retained(1));
    assert_eq!(mixed(excess_capacity), Err(DrainPlanError::TooLarge));
}
#[test]
fn learner_completion_requires_unchanged_committed_membership_and_no_dispatch() {
    let previous = entry(1);
    let records = [
        previous.change.joint.clone(),
        previous.change.finalize.clone(),
    ];
    let plan = mixed(vec![retained(1)]).unwrap();
    let journal = Journal(plan.record(1).unwrap());
    let pending = recovered(&previous, &records, 1);
    assert_eq!(
        plan.next(&journal, &pending),
        Ok(MembershipDrainAction::Wait)
    );
    let complete = recovered(&previous, &records, 2);
    assert_eq!(
        plan.next(&journal, &complete),
        Ok(MembershipDrainAction::Completed)
    );
    let mut coordinator = MembershipDrainCoordinator::new(plan.clone(), 2).unwrap();
    let batch = coordinator
        .poll(&journal, 2, |g| (g == group(1)).then_some(&complete))
        .unwrap();
    assert_eq!(batch.observed_complete, 1);
    assert_eq!(batch.unavailable, 1);
    assert!(batch.requests.is_empty());
    let changed = recovered(&previous, &records[..1], 1);
    assert_eq!(
        plan.next(&journal, &changed),
        Err(DrainPlanError::StateChanged)
    );
    let batch = coordinator
        .poll(&journal, 2, |g| (g == group(1)).then_some(&changed))
        .unwrap();
    assert_eq!(batch.errors, vec![(group(1), DrainPlanError::StateChanged)]);
    assert_eq!(batch.observed_complete, 0);
}
