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
//! Administrative placement checks. These never replace journal or quorum rules.
use crate::{identity::*, membership::*};
use std::collections::BTreeSet;
mod planning;
pub use planning::*;
mod changes;
pub use changes::*;

pub const PLACEMENT_AUTHORIZATION_CONTRACT_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlacementError {
    InvalidPlan,
    WrongGroup,
    UnknownReplica(NodeId),
    WrongStore(NodeId),
    TooFewVotingDomains,
    DomainLossPreventsQuorum(FailureDomainId),
}
/// Bounded, nonblocking execution-time authorization. Implementations may deny
/// proposals but cannot grant votes, mutate membership or waive core admission.
/// Reconstruct policy from trusted deployment input on restart; placement labels
/// are operator assertions, not observations of physical independence.
pub trait PlacementAuthorizer {
    fn authorize(
        &self,
        group: GroupIdentity,
        current: &Membership,
        record: &ConfigurationRecord,
    ) -> Result<(), PlacementError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplicaPlacement {
    pub store: StoreIdentity,
    pub domain: FailureDomainId,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlacementRequirements {
    /// Learners never contribute to this count.
    pub minimum_voting_domains: usize,
    /// Every single configured voting domain may fail while a quorum remains.
    /// This does not claim tolerance of simultaneous multiple-domain failures.
    pub survive_any_single_domain_loss: bool,
}

/// Shared deterministic check used by native authorization and offline preview.
pub(crate) fn validate_configuration_placement(
    configuration: &Configuration,
    replicas: &std::collections::BTreeMap<NodeId, ReplicaPlacement>,
    requirements: PlacementRequirements,
) -> Result<(), PlacementError> {
    for (&node, store) in configuration
        .voter_stores()
        .iter()
        .chain(configuration.learners())
    {
        let placement = replicas
            .get(&node)
            .ok_or(PlacementError::UnknownReplica(node))?;
        if &placement.store != store {
            return Err(PlacementError::WrongStore(node));
        }
    }
    let domains: BTreeSet<_> = configuration
        .voter_stores()
        .keys()
        .map(|node| replicas[node].domain)
        .collect();
    if domains.len() < requirements.minimum_voting_domains {
        return Err(PlacementError::TooFewVotingDomains);
    }
    if requirements.survive_any_single_domain_loss {
        for domain in domains {
            let survivors = configuration
                .voter_stores()
                .keys()
                .copied()
                .filter(|node| replicas[node].domain != domain)
                .collect();
            if !configuration.policy().is_satisfied(&survivors) {
                return Err(PlacementError::DomainLossPreventsQuorum(domain));
            }
        }
    }
    Ok(())
}
