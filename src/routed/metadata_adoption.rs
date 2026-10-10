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
//! Full-owner adoption of a completed metadata-authority move.
use super::*;
use crate::metadata_transfer::{
    MetadataActivationStatus, MetadataMovePlan, MAX_METADATA_PLAN_BYTES,
    METADATA_ACTIVATION_STATUS_BYTES,
};
use crate::transfer::ContentDigest;

pub const METADATA_ADOPTING_ROUTED_SCHEMA: u64 = 5;
pub const MAX_METADATA_ADOPTION_BYTES: usize = MAX_ROUTED_COMMAND_BYTES;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataGrantStatus {
    pub owner: ParentGrantStatus,
    pub activation: MetadataActivationStatus,
}
/// Original quorum observations, authenticated by the host. This value binds
/// all source/target phases but its encoding is not a commitment certificate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnerMetadataAdoption {
    plan: MetadataMovePlan,
    responsibility: ResponsibilityIdentity,
    activation: MetadataActivationStatus,
}
impl OwnerMetadataAdoption {
    pub fn new(
        plan: MetadataMovePlan,
        responsibility: ResponsibilityIdentity,
        activation: MetadataActivationStatus,
    ) -> Result<Self, ApplicationError> {
        let value = Self {
            plan,
            responsibility,
            activation,
        };
        value.validate()?;
        Ok(value)
    }
    fn validate(&self) -> Result<(), ApplicationError> {
        self.activation.encode()?;
        let source = self.activation.publication.imported.source;
        if source.source != self.plan.source()
            || source.target != self.plan.target()
            || source.plan_digest
                != ContentDigest::sha256(&self.plan.encode(MAX_METADATA_PLAN_BYTES)?)
            || !self
                .plan
                .manifests()
                .iter()
                .any(|m| m.input().responsibility == self.responsibility)
        {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(())
    }
    pub fn plan(&self) -> &MetadataMovePlan {
        &self.plan
    }
    pub fn activation(&self) -> MetadataActivationStatus {
        self.activation
    }
    pub fn before(&self) -> &ResponsibilityManifest {
        self.plan
            .manifests()
            .iter()
            .find(|m| m.input().responsibility == self.responsibility)
            .expect("checked selection")
    }
    pub fn after(&self) -> ResponsibilityManifest {
        self.plan
            .updated_manifests()
            .into_iter()
            .find(|m| m.input().responsibility == self.responsibility)
            .expect("checked selection")
    }
    pub fn encode(&self, maximum: usize) -> Result<Vec<u8>, ApplicationError> {
        self.validate()?;
        let plan = self.plan.encode(MAX_METADATA_PLAN_BYTES)?;
        let len = 36 + METADATA_ACTIVATION_STATUS_BYTES + plan.len();
        if len > maximum.min(MAX_METADATA_ADOPTION_BYTES) {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut bytes = Vec::with_capacity(len);
        bytes.extend(b"VBMAAD01");
        put_responsibility(&mut bytes, self.responsibility);
        bytes.extend(self.activation.encode()?);
        bytes.extend((plan.len() as u32).to_le_bytes());
        bytes.extend(plan);
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, ApplicationError> {
        if bytes.len() > MAX_METADATA_ADOPTION_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBMAAD01" {
            return Err(ApplicationError::InvalidCommand);
        }
        let responsibility = r.responsibility()?;
        let activation =
            MetadataActivationStatus::decode(r.take(METADATA_ACTIVATION_STATUS_BYTES)?)?;
        let n = r.u32()? as usize;
        let plan = MetadataMovePlan::decode(r.take(n)?)?;
        if !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        Self::new(plan, responsibility, activation)
    }
}
impl<A: CheckpointStateMachine, P: PartitionPolicy + Clone> RoutedApplication<A, P> {
    /// Original adoption outcome, including both metadata index domains.
    /// Foreign use requires an authenticated owner quorum observation.
    pub fn metadata_adoption(&self, operation: OperationId) -> Option<MetadataGrantStatus> {
        self.parent_adoptions.iter().find_map(|record| {
            if record.status.operation != operation {
                return None;
            }
            let ParentAdoptionCommand::Metadata(command) = &record.command else {
                return None;
            };
            Some(MetadataGrantStatus {
                owner: record.status,
                activation: command.activation(),
            })
        })
    }
    /// Select before bootstrap. Shares the bounded ordered parent/grant ledger;
    /// old profiles keep their exact command and checkpoint behavior.
    #[allow(clippy::result_large_err)]
    pub fn with_metadata_authority_adoption(
        self,
        maximum: usize,
    ) -> Result<Self, (ApplicationError, Self)> {
        if maximum == 0
            || maximum > MAX_PARENT_ADOPTIONS
            || self
                .readiness_requirements()
                .snapshot_bytes
                .checked_add(2 + maximum * (28 + MAX_METADATA_ADOPTION_BYTES))
                .is_none_or(|n| n > MAX_ROUTED_CHECKPOINT_BYTES)
        {
            return Err((ApplicationError::InvalidCommand, self));
        }
        let mut next = self.with_cross_authority_parent_adoption(maximum)?;
        next.metadata_adoption = true;
        next.binding[..8].copy_from_slice(b"VBROWN05");
        Ok(next)
    }
}
