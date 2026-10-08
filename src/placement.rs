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
