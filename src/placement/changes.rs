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
//! Explicit recommendations for ordinary learner/joint/final transitions.
use super::*;
use crate::quorum::{Limits, Policy, Tree};
use std::collections::BTreeMap;

/// A preparatory learner addition and the intended replacement policy.
/// Neither field authorizes promotion; plan again from recovered/current
/// membership after admitting the learner and establishing its readiness.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlannedReplacement {
    pub learner: PlannedLearner,
    pub target_policy: Policy,
}

/// Replace exactly one voter leaf without changing any branch or edge weight.
/// The selected provider recommends only the new non-voting learner.
pub fn plan_replacement<P: PlacementPlanner, A: PlacementAuthorizer>(
    planner: &P,
    authorizer: &A,
    request: PlacementRequest<'_>,
    retiring: NodeId,
    learner_operation: OperationId,
) -> Result<PlannedReplacement, PlacementPlanningError> {
    request.validate()?;
    if !request
        .current
        .stable()
        .policy()
        .voters()
        .contains(&retiring)
    {
        return Err(PlacementPlanningError::InvalidTarget);
    }
    let learner = plan_learner(planner, authorizer, request, learner_operation)?;
    let mut tree = request.current.stable().policy().tree().clone();
    replace_leaf(&mut tree, retiring, learner.recommendation.node);
    let target_policy =
        Policy::new(tree, Limits::default()).map_err(|_| PlacementPlanningError::InvalidTarget)?;
    Ok(PlannedReplacement {
        learner,
        target_policy,
    })
}

fn replace_leaf(tree: &mut Tree, old: NodeId, new: NodeId) {
    match tree {
        Tree::Voter(node) if *node == old => *node = new,
        Tree::Voter(_) => (),
        Tree::Majority(children) => {
            for child in children {
                replace_leaf(child, old, new);
            }
        }
        Tree::Weighted(children) => {
            for child in children {
                replace_leaf(&mut child.node, old, new);
            }
        }
    }
}

/// Treatment in the final configuration, never permission to decommission a
/// voter before final commitment and the normal retirement protocol.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemovedVoters {
    Retire,
    RetainAsLearners,
}

/// Proposed records, not accepted work. Normal execution-time authorization
/// and readiness still apply. Submit `finalize` only after `joint` commits.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlannedVoterChange {
    pub joint: ConfigurationRecord,
    pub finalize: ConfigurationRecord,
}

/// Build a joint transition to an explicitly chosen validated policy. Only
/// current exact voter/learner stores may appear in the target; no live load
/// sample silently changes quorum weights or grants voting authority.
pub fn plan_voter_change<A: PlacementAuthorizer>(
    authorizer: &A,
    request: PlacementRequest<'_>,
    target_policy: Policy,
    removed: RemovedVoters,
    operation: OperationId,
) -> Result<PlannedVoterChange, PlacementPlanningError> {
    request.validate()?;
    validate_operation(request.current, operation)?;
    let stable = request.current.stable();
    if &target_policy == stable.policy() {
        return Err(PlacementPlanningError::InvalidTarget);
    }
    let joint_id = next_configuration(request.current.id())?;
    let final_id = next_configuration(joint_id)?;
    let voters = target_stores(request, &target_policy)?;
    let mut learners = stable.learners().clone();
    for node in voters.keys() {
        learners.remove(node);
    }
    if removed == RemovedVoters::RetainAsLearners {
        for (&node, &store) in stable.voter_stores() {
            if !voters.contains_key(&node) {
                learners.insert(node, store);
            }
        }
    }
    let next = Configuration::new(final_id, target_policy, voters, learners)
        .map_err(PlacementPlanningError::Membership)?;
    let joint = ConfigurationRecord {
        operation,
        expected: request.current.id(),
        change: ConfigurationChange::Joint { id: joint_id, next },
    };
    authorizer
        .authorize(request.snapshot.group, request.current, &joint)
        .map_err(PlacementPlanningError::Authorization)?;
    Ok(PlannedVoterChange {
        joint,
        finalize: ConfigurationRecord {
            operation,
            expected: joint_id,
            change: ConfigurationChange::Final { id: final_id },
        },
    })
}

fn target_stores(
    request: PlacementRequest<'_>,
    policy: &Policy,
) -> Result<BTreeMap<NodeId, StoreIdentity>, PlacementPlanningError> {
    let stable = request.current.stable();
    policy
        .voters()
        .iter()
        .map(|node| {
            if let Some(&store) = stable.voter_stores().get(node) {
                return Ok((*node, store));
            }
            let store =
                stable
                    .learners()
                    .get(node)
                    .copied()
                    .ok_or(PlacementPlanningError::Membership(
                        MembershipError::UnpreparedVoter,
                    ))?;
            // Promotion uses an already allocated store: it requires neither
            // another replica slot nor another full copy's free space.
            if !request
                .snapshot
                .candidates
                .iter()
                .any(|c| c.node == *node && c.placement.store == store && c.enabled)
            {
                return Err(PlacementPlanningError::InvalidTarget);
            }
            Ok((*node, store))
        })
        .collect()
}
