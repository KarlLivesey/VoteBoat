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
use std::{cell::Cell, collections::BTreeMap};
use voteboat::{
    identity::*, log::Bootstrap, membership::*, placement::*, quorum::*, runtime::MonoTime,
};
fn node(n: u64) -> NodeId {
    NodeId::new(n).unwrap()
}
fn store(n: u64) -> StoreIdentity {
    StoreIdentity {
        id: StoreId::new(n as u128).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    }
}
fn group() -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(1).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
fn current() -> Membership {
    Membership::replay(
        &Bootstrap {
            group: group(),
            configuration: ConfigurationId::new(1).unwrap(),
            policy: Policy::new(
                Tree::Majority(vec![
                    Tree::Voter(node(1)),
                    Tree::Voter(node(2)),
                    Tree::Voter(node(3)),
                ]),
                Limits::default(),
            )
            .unwrap(),
            voter_stores: (1..=3).map(|n| (node(n), store(n))).collect(),
        },
        &[],
        0,
    )
    .unwrap()
}
fn candidates() -> Vec<PlacementCandidate> {
    (1..=6)
        .map(|n| PlacementCandidate {
            node: node(n),
            placement: ReplicaPlacement {
                store: store(n),
                domain: FailureDomainId::new(n).unwrap(),
            },
            enabled: true,
            free_bytes: 1000,
            free_replica_slots: 2,
            load_permille: 100,
        })
        .collect()
}
fn request<'a>(
    current: &'a Membership,
    candidates: &'a [PlacementCandidate],
) -> PlacementRequest<'a> {
    PlacementRequest {
        snapshot: PlacementSnapshot {
            group: group(),
            configuration: current.id(),
            generation: PlacementSampleGeneration::new(1).unwrap(),
            observed_at: MonoTime(1),
            expires_at: MonoTime(100),
            candidates,
        },
        current,
        now: MonoTime(2),
        minimum_free_bytes: 500,
    }
}
fn recommendation(request: PlacementRequest<'_>, n: u64) -> PlacementRecommendation {
    PlacementRecommendation {
        group: request.snapshot.group,
        configuration: request.current.id(),
        sample: request.snapshot.generation,
        node: node(n),
        store: store(n),
    }
}
struct HostPlanner {
    calls: Cell<usize>,
    chosen: PlacementRecommendation,
}
impl PlacementPlanner for HostPlanner {
    fn select(
        &self,
        _: PlacementRequest<'_>,
    ) -> Result<PlacementRecommendation, PlacementPlanningError> {
        self.calls.set(self.calls.get() + 1);
        Ok(self.chosen)
    }
}
struct HostAuthorizer {
    calls: Cell<usize>,
    deny: bool,
}
impl PlacementAuthorizer for HostAuthorizer {
    fn authorize(
        &self,
        _: GroupIdentity,
        _: &Membership,
        _: &ConfigurationRecord,
    ) -> Result<(), PlacementError> {
        self.calls.set(self.calls.get() + 1);
        if self.deny {
            Err(PlacementError::InvalidPlan)
        } else {
            Ok(())
        }
    }
}
#[test]
fn downstream_planner_builds_only_an_authorized_learner_record_and_does_not_mutate_membership() {
    let current = current();
    let candidates = candidates();
    let r = request(&current, &candidates);
    let planner = HostPlanner {
        calls: Cell::new(0),
        chosen: recommendation(r, 4),
    };
    let mut auth = HostAuthorizer {
        calls: Cell::new(0),
        deny: false,
    };
    let before = current.clone();
    let original = candidates.clone();
    let planned = plan_learner(&planner, &auth, r, OperationId::new(10).unwrap()).unwrap();
    assert_eq!(planned.record.expected, current.id());
    let ConfigurationChange::Learners(next) = &planned.record.change else {
        panic!("changed policy");
    };
    assert_eq!(next.policy(), current.stable().policy());
    assert_eq!(next.voter_stores(), current.stable().voter_stores());
    assert_eq!(next.learners(), &BTreeMap::from([(node(4), store(4))]));
    assert_eq!(next.id().get(), 2);
    assert_eq!(current, before);
    assert_eq!(candidates, original);
    auth.deny = true;
    assert_eq!(
        plan_learner(&planner, &auth, r, OperationId::new(10).unwrap()),
        Err(PlacementPlanningError::Authorization(
            PlacementError::InvalidPlan
        ))
    );
}
#[test]
fn common_gate_refuses_forged_provider_scope_sample_store_and_existing_or_disabled_nodes() {
    let current = current();
    let mut candidates = candidates();
    candidates[5].enabled = false;
    let r = request(&current, &candidates);
    let auth = HostAuthorizer {
        calls: Cell::new(0),
        deny: false,
    };
    for field in 0..7 {
        let mut chosen = recommendation(r, 4);
        match field {
            0 => chosen.group.incarnation = GroupIncarnation::new(2).unwrap(),
            1 => chosen.configuration = ConfigurationId::new(2).unwrap(),
            2 => chosen.sample = PlacementSampleGeneration::new(2).unwrap(),
            3 => chosen.store = store(99),
            4 => chosen = recommendation(r, 1),
            5 => chosen = recommendation(r, 6),
            _ => chosen = recommendation(r, 99),
        }
        let p = HostPlanner {
            calls: Cell::new(0),
            chosen,
        };
        assert_eq!(
            plan_learner(&p, &auth, r, OperationId::new(10).unwrap()),
            Err(PlacementPlanningError::ProviderViolation)
        );
    }
    assert_eq!(auth.calls.get(), 0);
}
#[test]
fn invalid_or_stale_samples_fail_before_planner_or_authorizer() {
    let current = current();
    let candidates = candidates();
    let valid = request(&current, &candidates);
    let planner = HostPlanner {
        calls: Cell::new(0),
        chosen: recommendation(valid, 4),
    };
    let auth = HostAuthorizer {
        calls: Cell::new(0),
        deny: false,
    };
    for field in 0..4 {
        let mut r = valid;
        match field {
            0 => r.snapshot.configuration = ConfigurationId::new(9).unwrap(),
            1 => r.now = MonoTime(0),
            2 => r.now = MonoTime(100),
            _ => r.snapshot.candidates = &[],
        }
        let expected = match field {
            0 => PlacementPlanningError::StaleConfiguration,
            1 => PlacementPlanningError::TimeWentBack,
            2 => PlacementPlanningError::Expired,
            _ => PlacementPlanningError::InvalidSample,
        };
        assert_eq!(
            plan_learner(&planner, &auth, r, OperationId::new(10).unwrap()),
            Err(expected)
        );
    }
    for field in 0..4 {
        let mut bad = candidates.clone();
        match field {
            0 => bad[4].node = node(4),
            1 => bad[4].placement.store = store(4),
            2 => bad[0].placement.store = store(99),
            _ => bad[4].load_permille = 1001,
        }
        assert_eq!(
            plan_learner(
                &planner,
                &auth,
                request(&current, &bad),
                OperationId::new(10).unwrap()
            ),
            Err(PlacementPlanningError::InvalidSample)
        );
    }
    assert_eq!(planner.calls.get(), 0);
    assert_eq!(auth.calls.get(), 0);
}
#[cfg(feature = "native")]
#[test]
fn native_rank_is_deterministic_domain_then_load_capacity_node_and_authorization_is_independent() {
    use voteboat::native::{
        placement::NativePlacementAuthorizer, placement_planning::NativePlacementPlanner,
    };
    let current = current();
    let mut candidates = candidates();
    candidates[3].placement.domain = FailureDomainId::new(1).unwrap();
    candidates[3].load_permille = 0;
    candidates[4].load_permille = 200;
    candidates[5].load_permille = 100;
    let planner = NativePlacementPlanner;
    let r = request(&current, &candidates);
    assert_eq!(
        planner.select(r).unwrap().node,
        node(6),
        "new domain beats existing domain"
    );
    candidates[4].load_permille = 100;
    candidates[4].free_bytes = 2000;
    assert_eq!(
        planner.select(request(&current, &candidates)).unwrap().node,
        node(5),
        "capacity breaks equal load"
    );
    candidates[5].free_bytes = 2000;
    let chosen = planner.select(request(&current, &candidates)).unwrap();
    assert_eq!(chosen.node, node(5));
    candidates.reverse();
    assert_eq!(
        planner.select(request(&current, &candidates)).unwrap(),
        chosen
    );
    let assignments = candidates.iter().map(|c| (c.node, c.placement)).collect();
    let auth = NativePlacementAuthorizer::new(
        group(),
        assignments,
        PlacementRequirements {
            minimum_voting_domains: 3,
            survive_any_single_domain_loss: true,
        },
    )
    .unwrap();
    let planned = plan_learner(
        &planner,
        &auth,
        request(&current, &candidates),
        OperationId::new(10).unwrap(),
    )
    .unwrap();
    assert_eq!(planned.recommendation, chosen);
    for c in &mut candidates {
        if c.node.get() > 3 {
            c.free_replica_slots = 0;
        }
    }
    assert_eq!(
        planner.select(request(&current, &candidates)),
        Err(PlacementPlanningError::NoCandidate)
    );
}

#[test]
fn joint_transition_history_exhaustion_and_id_overflow_refuse_before_recommendation() {
    let base = current();
    let candidates = candidates();
    let operation = OperationId::new(100).unwrap();
    let joint = Membership::from_checkpoint(
        base.stable().clone(),
        Some(JointConfiguration {
            operation,
            id: ConfigurationId::new(2).unwrap(),
            index: 1,
            next: Configuration::new(
                ConfigurationId::new(3).unwrap(),
                base.stable().policy().clone(),
                base.stable().voter_stores().clone(),
                BTreeMap::new(),
            )
            .unwrap(),
        }),
        1,
        [operation].into(),
        1,
    )
    .unwrap();
    let r = request(&joint, &candidates);
    let planner = HostPlanner {
        calls: Cell::new(0),
        chosen: recommendation(r, 4),
    };
    let auth = HostAuthorizer {
        calls: Cell::new(0),
        deny: false,
    };
    assert_eq!(
        plan_learner(&planner, &auth, r, OperationId::new(20000).unwrap()),
        Err(PlacementPlanningError::TransitionActive)
    );
    let full = Membership::from_checkpoint(
        base.stable().clone(),
        None,
        MAX_CONFIGURATION_OPERATIONS as u64,
        (1..=MAX_CONFIGURATION_OPERATIONS as u128)
            .map(|n| OperationId::new(n).unwrap())
            .collect(),
        MAX_CONFIGURATION_OPERATIONS as u64,
    )
    .unwrap();
    assert_eq!(
        plan_learner(
            &planner,
            &auth,
            request(&full, &candidates),
            OperationId::new(20000).unwrap()
        ),
        Err(PlacementPlanningError::Membership(
            MembershipError::HistoryFull
        ))
    );
    assert_eq!(
        plan_learner(
            &planner,
            &auth,
            request(&full, &candidates),
            OperationId::new(10).unwrap()
        ),
        Err(PlacementPlanningError::Membership(
            MembershipError::ReusedOperation
        ))
    );
    let exhausted = Membership::from_checkpoint(
        Configuration::new(
            ConfigurationId::new(u64::MAX).unwrap(),
            base.stable().policy().clone(),
            base.stable().voter_stores().clone(),
            BTreeMap::new(),
        )
        .unwrap(),
        None,
        1,
        [operation].into(),
        1,
    )
    .unwrap();
    assert_eq!(
        plan_learner(
            &planner,
            &auth,
            request(&exhausted, &candidates),
            OperationId::new(20000).unwrap()
        ),
        Err(PlacementPlanningError::Exhausted)
    );
    assert_eq!(planner.calls.get(), 0);
    assert_eq!(auth.calls.get(), 0);
}
