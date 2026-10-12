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
//! Original consumed Claim and fresh-image provenance, separate from recovery data.
use super::*;
impl LogicalEndpointHop {
    pub fn mapping(self) -> LogicalStoreMapping {
        self.mapping
    }
    pub fn representative(self) -> RepresentativeBinding {
        self.representative
    }
    pub fn claim(self) -> LogicalReceipt {
        self.claim
    }
    pub fn barrier(self) -> crate::raft::ReadBarrier {
        self.barrier
    }
    pub fn child_applied(self) -> u64 {
        self.child_applied
    }
    pub fn child_configuration(self) -> ConfigurationId {
        self.child_configuration
    }
    pub(super) fn observed(
        image: &LogicalImage,
        claim: LogicalReceipt,
        barrier: crate::raft::ReadBarrier,
    ) -> Result<Self, LogicalEndpointError> {
        if image
            .child_membership
            .voter_store(image.representative.physical.node)
            != Some(image.representative.physical.store)
        {
            return Err(LogicalEndpointError::InvalidObservation);
        }
        let hop = Self {
            mapping: image.mapping,
            representative: image.representative,
            claim,
            barrier,
            child_applied: image.child_applied,
            child_configuration: image.child_membership.id(),
            eligible: image.representative.physical,
        };
        hop.validate()?;
        Ok(hop)
    }
    pub(super) fn owner(&self) -> LocalIdentity {
        LocalIdentity {
            node: self.mapping.voter,
            store: StoreBinding {
                identity: self.mapping.store,
                session: self.representative.logical_session,
            },
        }
    }
    pub(super) fn immediate(&self) -> LocalIdentity {
        LocalIdentity {
            node: self.representative.physical.node,
            store: StoreBinding {
                identity: self.representative.physical.store,
                session: self.representative.physical_session,
            },
        }
    }
    pub(super) fn validate(&self) -> Result<(), LogicalEndpointError> {
        if self.claim.outcome != LogicalOutcome::Claimed
            || self.claim.epoch != Some(self.representative.epoch)
            || self.claim.first_index == 0
            || self.claim.first_index > self.claim.index
            || self.barrier.group() != self.mapping.child
            || self.barrier.index() < self.claim.first_index
            || self.child_applied < self.barrier.index()
            || self.eligible != self.representative.physical
        {
            return Err(LogicalEndpointError::InvalidObservation);
        }
        Ok(())
    }
}
