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
//! Immutable trusted intent scope for the existing Node authorization callback.
use crate::{identity::*, membership::*, native::placement::*, placement::*, raft::*};

pub const MAX_ADMINISTRATION_INTENTS: usize = 64;
pub const MAX_ADMINISTRATION_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdministrationPlanError {
    InvalidRequirements,
    InvalidIntents,
    DuplicateIntent,
    TooLarge,
}
#[derive(Debug)]
pub struct AdministrationPlanRejected<P = NativePlacementAuthorizer> {
    pub reason: AdministrationPlanError,
    pub group: GroupIdentity,
    pub requirements: ReadinessRequirements,
    pub placement: P,
    pub intents: Vec<ConfigurationRecord>,
}
/// One exact group, application envelope and finite set of trusted intents.
/// This is host authorization, not consensus, readiness or a durability receipt.
/// Reconstruct from trusted host input on restart; do not derive it from traffic.
#[derive(Debug)]
pub struct NativeAdministrationPlan<P = NativePlacementAuthorizer> {
    group: GroupIdentity,
    placement: P,
    requirements: ReadinessRequirements,
    intents: Vec<ConfigurationRecord>,
}
impl<P: PlacementAuthorizer> NativeAdministrationPlan<P> {
    pub fn new(
        group: GroupIdentity,
        placement: P,
        requirements: ReadinessRequirements,
        intents: Vec<ConfigurationRecord>,
    ) -> Result<Self, Box<AdministrationPlanRejected<P>>> {
        let reason = if requirements.application_schema == 0
            || requirements.command_bytes == 0
            || requirements.snapshot_bytes == 0
        {
            Some(AdministrationPlanError::InvalidRequirements)
        } else if intents.is_empty() {
            Some(AdministrationPlanError::InvalidIntents)
        } else if intents.len() > MAX_ADMINISTRATION_INTENTS
            || intents.capacity() > MAX_ADMINISTRATION_INTENTS
            || intents
                .iter()
                .try_fold(0usize, |bytes, record| {
                    bytes.checked_add(record.retained_bytes())
                })
                .is_none_or(|bytes| bytes > MAX_ADMINISTRATION_BYTES)
        {
            Some(AdministrationPlanError::TooLarge)
        } else if intents.iter().enumerate().any(|(i, record)| {
            intents[..i].iter().any(|other| {
                (record.operation, record.expected) == (other.operation, other.expected)
            })
        }) {
            Some(AdministrationPlanError::DuplicateIntent)
        } else {
            None
        };
        if let Some(reason) = reason {
            return Err(Box::new(AdministrationPlanRejected {
                reason,
                group,
                requirements,
                placement,
                intents,
            }));
        }
        Ok(Self {
            group,
            placement,
            requirements,
            intents,
        })
    }
    pub fn group(&self) -> GroupIdentity {
        self.group
    }
    pub fn requirements(&self) -> ReadinessRequirements {
        self.requirements
    }
    pub fn intents(&self) -> &[ConfigurationRecord] {
        &self.intents
    }
    /// Bounded, synchronous execution-time check. Select the current plan for
    /// each poll; replacing it does not preserve previously queued permission.
    /// Exact record equality includes operation, expected head and entire target.
    /// Node/core still check journal, live readiness bindings and provider sizing.
    pub fn authorize(
        &self,
        group: GroupIdentity,
        current: &Membership,
        proposal: &ConfigurationProposal,
    ) -> Result<(), ConfigurationProposalError> {
        if group != self.group() || !self.intents.iter().any(|r| r == &proposal.record) {
            return Err(ConfigurationProposalError::AuthenticationRequired);
        }
        if proposal.requirements != self.requirements {
            return Err(ConfigurationProposalError::Readiness(
                ReadinessError::InvalidRequirements,
            ));
        }
        self.placement
            .authorize(group, current, &proposal.record)
            .map_err(ConfigurationProposalError::Placement)
    }
}
