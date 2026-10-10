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
use voteboat::log::{EntryPayload, LogEntry};

fn auth() -> HostAuthorizer {
    HostAuthorizer {
        calls: Cell::new(0),
        deny: false,
    }
}
fn operation(n: u128) -> OperationId {
    OperationId::new(n).unwrap()
}
fn policy(voters: &[u64]) -> Policy {
    Policy::new(
        Tree::Majority(voters.iter().map(|n| Tree::Voter(node(*n))).collect()),
        Limits::default(),
    )
    .unwrap()
}
fn append(
    current: &Membership,
    record: ConfigurationRecord,
    committed: u64,
) -> Result<Membership, MembershipError> {
    let base = bootstrap();
    let index = current.last_configuration_index() + 1;
    Membership::replay_from(
        &base,
        Some(current),
        index - 1,
        &[LogEntry {
            index,
            term: 1,
            payload: EntryPayload::Configuration(Box::new(record)),
        }],
        committed,
    )
}
fn bootstrap() -> Bootstrap {
    Bootstrap {
        group: group(),
        configuration: ConfigurationId::new(1).unwrap(),
        policy: policy(&[1, 2, 3]),
        voter_stores: (1..=3).map(|n| (node(n), store(n))).collect(),
    }
}
fn learned() -> Membership {
    let base = current();
    let c = candidates();
    let p = HostPlanner {
        calls: Cell::new(0),
        chosen: recommendation(request(&base, &c), 4),
    };
    let plan = plan_learner(&p, &auth(), request(&base, &c), operation(10)).unwrap();
    append(&base, plan.record, 1).unwrap()
}

#[test]
fn replacement_preserves_recursive_edges_and_promotes_only_after_learner_admission() {
    let tree = Tree::Weighted(vec![
        WeightedChild {
            weight: 7,
            node: Tree::Majority(vec![Tree::Voter(node(1)), Tree::Voter(node(2))]),
        },
        WeightedChild {
            weight: 4,
            node: Tree::Voter(node(3)),
        },
    ]);
    let base = Membership::from_checkpoint(
        Configuration::new(
            ConfigurationId::new(1).unwrap(),
            Policy::new(tree, Limits::default()).unwrap(),
            (1..=3).map(|n| (node(n), store(n))).collect(),
            BTreeMap::new(),
        )
        .unwrap(),
        None,
        0,
        Default::default(),
        0,
    )
    .unwrap();
    let c = candidates();
    let p = HostPlanner {
        calls: Cell::new(0),
        chosen: recommendation(request(&base, &c), 4),
    };
    let plan = plan_replacement(&p, &auth(), request(&base, &c), node(2), operation(10)).unwrap();
    let expected = Tree::Weighted(vec![
        WeightedChild {
            weight: 7,
            node: Tree::Majority(vec![Tree::Voter(node(1)), Tree::Voter(node(4))]),
        },
        WeightedChild {
            weight: 4,
            node: Tree::Voter(node(3)),
        },
    ]);
    assert_eq!(plan.target_policy.tree(), &expected);
    let ConfigurationChange::Learners(learner) = &plan.learner.record.change else {
        panic!("not a learner");
    };
    assert_eq!(learner.policy(), base.stable().policy());
    assert_eq!(learner.learners(), &BTreeMap::from([(node(4), store(4))]));
    assert_eq!(
        plan_voter_change(
            &auth(),
            request(&base, &c),
            plan.target_policy,
            RemovedVoters::Retire,
            operation(11)
        ),
        Err(PlacementPlanningError::Membership(
            MembershipError::UnpreparedVoter
        ))
    );
    assert_eq!(p.calls.get(), 1);
}

#[test]
fn replacement_replays_joint_before_final_and_retains_identity_through_checkpoint() {
    let base = current();
    let c = candidates();
    let planner = HostPlanner {
        calls: Cell::new(0),
        chosen: recommendation(request(&base, &c), 4),
    };
    let replacement = plan_replacement(
        &planner,
        &auth(),
        request(&base, &c),
        node(3),
        operation(10),
    )
    .unwrap();
    let learner_record = replacement.learner.record;
    let learner = append(&base, learner_record.clone(), 1).unwrap();
    let plan = plan_voter_change(
        &auth(),
        request(&learner, &c),
        replacement.target_policy,
        RemovedVoters::Retire,
        operation(11),
    )
    .unwrap();
    let joint = append(&learner, plan.joint.clone(), 1).unwrap();
    assert!(joint.is_voter(node(3)) && joint.is_voter(node(4)));
    assert!(!joint.is_satisfied(&[node(2), node(3)].into()));
    assert!(!joint.is_satisfied(&[node(2), node(4)].into()));
    assert!(joint.is_satisfied(&[node(1), node(2)].into()));
    let premature = [learner_record, plan.joint.clone(), plan.finalize.clone()]
        .into_iter()
        .enumerate()
        .map(|(i, record)| LogEntry {
            index: i as u64 + 1,
            term: 1,
            payload: EntryPayload::Configuration(Box::new(record)),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        Membership::replay(&bootstrap(), &premature, 1),
        Err(MembershipError::JointNotCommitted)
    );
    let restored = Membership::from_checkpoint(
        joint.stable().clone(),
        joint.joint().cloned(),
        joint.last_configuration_index(),
        joint.operations().clone(),
        2,
    )
    .unwrap();
    let done = append(&restored, plan.finalize, 2).unwrap();
    assert!(!done.is_voter(node(3)) && done.is_voter(node(4)));
    assert_eq!(done.replica_store(node(3)), None);
    assert_eq!(done.voter_store(node(4)), Some(store(4)));
    assert_eq!(done.id().get(), 4);
    assert!(
        done.operations().contains(&operation(10)) && done.operations().contains(&operation(11))
    );
    assert_eq!(
        append(&base, plan.joint, 0),
        Err(MembershipError::StaleConfiguration)
    );
}

#[test]
fn removal_explicitly_retires_or_retains_old_voters_and_preserves_unrelated_learners() {
    let base = learned();
    let c = candidates();
    for removed in [RemovedVoters::Retire, RemovedVoters::RetainAsLearners] {
        let before = base.clone();
        let plan = plan_voter_change(
            &auth(),
            request(&base, &c),
            policy(&[1, 2]),
            removed,
            operation(11),
        )
        .unwrap();
        let joint = append(&base, plan.joint, 1).unwrap();
        let done = append(&joint, plan.finalize, 2).unwrap();
        assert_eq!(
            done.stable().voter_stores(),
            &BTreeMap::from([(node(1), store(1)), (node(2), store(2))])
        );
        assert_eq!(done.stable().learners().get(&node(4)), Some(&store(4)));
        assert_eq!(
            done.replica_store(node(3)),
            (removed == RemovedVoters::RetainAsLearners).then_some(store(3))
        );
        assert_eq!(base, before);
    }
}

#[test]
fn invalid_promotion_and_stale_samples_fail_before_authorization() {
    let base = learned();
    let mut c = candidates();
    let auth = auth();
    assert_eq!(
        plan_voter_change(
            &auth,
            request(&base, &c),
            policy(&[1, 2, 5]),
            RemovedVoters::Retire,
            operation(11)
        ),
        Err(PlacementPlanningError::Membership(
            MembershipError::UnpreparedVoter
        ))
    );
    assert_eq!(
        plan_voter_change(
            &auth,
            request(&base, &c),
            base.stable().policy().clone(),
            RemovedVoters::Retire,
            operation(11)
        ),
        Err(PlacementPlanningError::InvalidTarget)
    );
    c[3].enabled = false;
    assert_eq!(
        plan_voter_change(
            &auth,
            request(&base, &c),
            policy(&[1, 2, 4]),
            RemovedVoters::Retire,
            operation(11)
        ),
        Err(PlacementPlanningError::InvalidTarget)
    );
    c[3].enabled = true;
    c[3].placement.store = store(99);
    assert_eq!(
        plan_voter_change(
            &auth,
            request(&base, &c),
            policy(&[1, 2, 4]),
            RemovedVoters::Retire,
            operation(11)
        ),
        Err(PlacementPlanningError::InvalidSample)
    );
    c[3].placement.store = store(4);
    let mut expired = request(&base, &c);
    expired.now = MonoTime(100);
    assert_eq!(
        plan_voter_change(
            &auth,
            expired,
            policy(&[1, 2, 4]),
            RemovedVoters::Retire,
            operation(11)
        ),
        Err(PlacementPlanningError::Expired)
    );
    assert_eq!(auth.calls.get(), 0);
}

#[test]
fn existing_learner_promotion_needs_no_new_slot_and_denial_accepts_nothing() {
    let base = learned();
    let mut c = candidates();
    c[3].free_bytes = 0;
    c[3].free_replica_slots = 0;
    let before = base.clone();
    let mut auth = auth();
    let plan = plan_voter_change(
        &auth,
        request(&base, &c),
        policy(&[1, 2, 4]),
        RemovedVoters::Retire,
        operation(11),
    )
    .unwrap();
    let ConfigurationChange::Joint { next, .. } = plan.joint.change else {
        panic!("not joint");
    };
    assert_eq!(next.voter_stores().get(&node(4)), Some(&store(4)));
    assert!(!next.learners().contains_key(&node(4)));
    auth.deny = true;
    assert_eq!(
        plan_voter_change(
            &auth,
            request(&base, &c),
            policy(&[1, 2, 4]),
            RemovedVoters::Retire,
            operation(11)
        ),
        Err(PlacementPlanningError::Authorization(
            PlacementError::InvalidPlan
        ))
    );
    assert_eq!(base, before);
}

#[test]
fn transition_operation_and_two_id_boundaries_fail_without_authorizing() {
    let base = learned();
    let c = candidates();
    let auth = auth();
    assert_eq!(
        plan_voter_change(
            &auth,
            request(&base, &c),
            policy(&[1, 2, 4]),
            RemovedVoters::Retire,
            operation(10)
        ),
        Err(PlacementPlanningError::Membership(
            MembershipError::ReusedOperation
        ))
    );
    let plan = plan_voter_change(
        &auth,
        request(&base, &c),
        policy(&[1, 2, 4]),
        RemovedVoters::Retire,
        operation(11),
    )
    .unwrap();
    let joint = append(&base, plan.joint, 1).unwrap();
    assert_eq!(
        plan_voter_change(
            &auth,
            request(&joint, &c),
            policy(&[1, 2]),
            RemovedVoters::Retire,
            operation(12)
        ),
        Err(PlacementPlanningError::TransitionActive)
    );
    for id in [u64::MAX - 1, u64::MAX] {
        let exhausted = Membership::from_checkpoint(
            Configuration::new(
                ConfigurationId::new(id).unwrap(),
                base.stable().policy().clone(),
                base.stable().voter_stores().clone(),
                base.stable().learners().clone(),
            )
            .unwrap(),
            None,
            1,
            [operation(10)].into(),
            1,
        )
        .unwrap();
        assert_eq!(
            plan_voter_change(
                &auth,
                request(&exhausted, &c),
                policy(&[1, 2, 4]),
                RemovedVoters::Retire,
                operation(11)
            ),
            Err(PlacementPlanningError::Exhausted)
        );
    }
    assert_eq!(auth.calls.get(), 1);
}

#[cfg(feature = "native")]
#[test]
fn native_replacement_selection_and_removal_respect_declared_failure_domains() {
    use voteboat::native::{
        placement::NativePlacementAuthorizer, placement_planning::NativePlacementPlanner,
    };
    let base = current();
    let c = candidates();
    let auth = NativePlacementAuthorizer::new(
        group(),
        c.iter().map(|c| (c.node, c.placement)).collect(),
        PlacementRequirements {
            minimum_voting_domains: 3,
            survive_any_single_domain_loss: true,
        },
    )
    .unwrap();
    let plan = plan_replacement(
        &NativePlacementPlanner,
        &auth,
        request(&base, &c),
        node(3),
        operation(10),
    )
    .unwrap();
    assert_eq!(plan.learner.recommendation.node, node(4));
    let learned = append(&base, plan.learner.record, 1).unwrap();
    assert!(plan_voter_change(
        &auth,
        request(&learned, &c),
        plan.target_policy,
        RemovedVoters::Retire,
        operation(11)
    )
    .is_ok());
    assert_eq!(
        plan_voter_change(
            &auth,
            request(&learned, &c),
            policy(&[1, 2]),
            RemovedVoters::Retire,
            operation(11)
        ),
        Err(PlacementPlanningError::Authorization(
            PlacementError::TooFewVotingDomains
        ))
    );
}

#[test]
fn replacement_and_voter_change_refuse_invalid_intent_and_full_history() {
    let base = current();
    let c = candidates();
    let planner = HostPlanner {
        calls: Cell::new(0),
        chosen: recommendation(request(&base, &c), 4),
    };
    let auth = auth();
    assert_eq!(
        plan_replacement(&planner, &auth, request(&base, &c), node(9), operation(10)),
        Err(PlacementPlanningError::InvalidTarget)
    );
    let full = Membership::from_checkpoint(
        base.stable().clone(),
        None,
        MAX_CONFIGURATION_OPERATIONS as u64,
        (1..=MAX_CONFIGURATION_OPERATIONS as u128)
            .map(operation)
            .collect(),
        MAX_CONFIGURATION_OPERATIONS as u64,
    )
    .unwrap();
    assert_eq!(
        plan_voter_change(
            &auth,
            request(&full, &c),
            policy(&[1, 2]),
            RemovedVoters::Retire,
            operation(20000)
        ),
        Err(PlacementPlanningError::Membership(
            MembershipError::HistoryFull
        ))
    );
    assert_eq!(planner.calls.get(), 0);
    assert_eq!(auth.calls.get(), 0);
}
