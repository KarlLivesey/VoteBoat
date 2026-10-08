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
//! Explicit bounded deployment placement authorization, without I/O or activation.
use crate::{identity::*, membership::*, placement::*};
use std::collections::{BTreeMap, BTreeSet};

pub const MAX_PLACEMENT_REPLICAS: usize = 4096;
pub const MAX_PLACEMENT_DOMAINS: usize = 64;
/// Immutable group-scoped deployment plan. Replace the caller-owned plan to
/// change authorization; queued operations consult the plan used at execution.
/// Does not provision routes/credentials or establish learner readiness.
#[derive(Debug)]
pub struct NativePlacementAuthorizer {
    group: GroupIdentity,
    replicas: BTreeMap<NodeId, ReplicaPlacement>,
    requirements: PlacementRequirements,
}
impl NativePlacementAuthorizer {
    /// Validates fixed resource ceilings before retaining the owned plan.
    /// Rejection returns the original assignments; no work or I/O is accepted.
    pub fn new(
        group: GroupIdentity,
        replicas: BTreeMap<NodeId, ReplicaPlacement>,
        requirements: PlacementRequirements,
    ) -> Result<Self, (PlacementError, BTreeMap<NodeId, ReplicaPlacement>)> {
        let domains: BTreeSet<_> = replicas
            .values()
            .take(MAX_PLACEMENT_REPLICAS + 1)
            .map(|r| r.domain)
            .collect();
        let stores: BTreeSet<_> = replicas
            .values()
            .take(MAX_PLACEMENT_REPLICAS + 1)
            .map(|r| (r.store.id, r.store.incarnation))
            .collect();
        if replicas.is_empty()
            || replicas.len() > MAX_PLACEMENT_REPLICAS
            || domains.len() > MAX_PLACEMENT_DOMAINS
            || requirements.minimum_voting_domains == 0
            || requirements.minimum_voting_domains > MAX_PLACEMENT_DOMAINS
            || requirements.minimum_voting_domains > domains.len()
            || stores.len() != replicas.len()
        {
            return Err((PlacementError::InvalidPlan, replicas));
        }
        Ok(Self {
            group,
            replicas,
            requirements,
        })
    }
    pub fn group(&self) -> GroupIdentity {
        self.group
    }
    pub fn replicas(&self) -> &BTreeMap<NodeId, ReplicaPlacement> {
        &self.replicas
    }
    pub fn requirements(&self) -> PlacementRequirements {
        self.requirements
    }
    fn configuration(&self, configuration: &Configuration) -> Result<(), PlacementError> {
        for (&node, store) in configuration
            .voter_stores()
            .iter()
            .chain(configuration.learners())
        {
            let placement = self
                .replicas
                .get(&node)
                .ok_or(PlacementError::UnknownReplica(node))?;
            if &placement.store != store {
                return Err(PlacementError::WrongStore(node));
            }
        }
        let domains: BTreeSet<_> = configuration
            .voter_stores()
            .keys()
            .map(|node| self.replicas[node].domain)
            .collect();
        if domains.len() < self.requirements.minimum_voting_domains {
            return Err(PlacementError::TooFewVotingDomains);
        }
        if self.requirements.survive_any_single_domain_loss {
            for domain in domains {
                let survivors = configuration
                    .voter_stores()
                    .keys()
                    .copied()
                    .filter(|node| self.replicas[node].domain != domain)
                    .collect();
                if !configuration.policy().is_satisfied(&survivors) {
                    return Err(PlacementError::DomainLossPreventsQuorum(domain));
                }
            }
        }
        Ok(())
    }
}
impl PlacementAuthorizer for NativePlacementAuthorizer {
    fn authorize(
        &self,
        group: GroupIdentity,
        current: &Membership,
        record: &ConfigurationRecord,
    ) -> Result<(), PlacementError> {
        if group != self.group {
            return Err(PlacementError::WrongGroup);
        }
        // Both accepted predicates and both replica sets remain obligations
        // during transition, including finalization and retained learners.
        self.configuration(current.stable())?;
        if let Some(joint) = current.joint() {
            self.configuration(&joint.next)?;
        }
        match &record.change {
            ConfigurationChange::Learners(next) | ConfigurationChange::Joint { next, .. } => {
                self.configuration(next)
            }
            ConfigurationChange::Final { .. } => Ok(()),
        }
    }
}
