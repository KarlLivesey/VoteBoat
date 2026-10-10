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
//! Owner adoption of an original foreign-directory locator update.
use super::*;
use crate::metadata_transfer::{
    MetadataLocatorStatus, MetadataLocatorUpdate, MAX_METADATA_LOCATOR_BYTES,
};
use crate::transfer::ContentDigest;

pub const LOCATOR_ADOPTING_ROUTED_SCHEMA: u64 = 6;
pub const MAX_METADATA_LOCATOR_ADOPTION_BYTES: usize = 44 + MAX_METADATA_LOCATOR_BYTES;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataLocatorGrantStatus {
    pub owner: ParentGrantStatus,
    pub metadata_configuration: ConfigurationId,
    pub locator: MetadataLocatorStatus,
}
/// Host-authenticated original directory observation. This carries a complete
/// update and checks its bindings; the encoding is not a commitment certificate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnerMetadataLocatorAdoption {
    update: MetadataLocatorUpdate,
    configuration: ConfigurationId,
    observation: MetadataLocatorStatus,
}
impl OwnerMetadataLocatorAdoption {
    pub fn new(
        update: MetadataLocatorUpdate,
        configuration: ConfigurationId,
        observation: MetadataLocatorStatus,
    ) -> Result<Self, ApplicationError> {
        let value = Self {
            update,
            configuration,
            observation,
        };
        value.validate()?;
        Ok(value)
    }
    fn validate(&self) -> Result<(), ApplicationError> {
        let status = self.observation;
        if status.index == 0
            || status.index == u64::MAX
            || status.authority != self.before().input().authority
            || status.responsibility != self.before().input().responsibility
            || status.generation != self.after().input().generation
            || status.activation != self.update.activation()
            || status.command_digest
                != ContentDigest::sha256(&self.update.encode(MAX_METADATA_LOCATOR_BYTES)?)
        {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(())
    }
    pub fn update(&self) -> &MetadataLocatorUpdate {
        &self.update
    }
    pub fn metadata_configuration(&self) -> ConfigurationId {
        self.configuration
    }
    pub fn observation(&self) -> MetadataLocatorStatus {
        self.observation
    }
    pub fn before(&self) -> &ResponsibilityManifest {
        self.update.before()
    }
    pub fn after(&self) -> ResponsibilityManifest {
        self.update.after()
    }
    pub(crate) fn status(&self, owner: ParentGrantStatus) -> MetadataLocatorGrantStatus {
        MetadataLocatorGrantStatus {
            owner,
            metadata_configuration: self.configuration,
            locator: self.observation,
        }
    }
    pub fn encode(&self, maximum: usize) -> Result<Vec<u8>, ApplicationError> {
        self.validate()?;
        let update = self.update.encode(MAX_METADATA_LOCATOR_BYTES)?;
        let len = 44 + update.len();
        if len > maximum.min(MAX_METADATA_LOCATOR_ADOPTION_BYTES) {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut bytes = Vec::with_capacity(len);
        bytes.extend(b"VBMLAD01");
        bytes.extend(self.configuration.get().to_le_bytes());
        bytes.extend(self.observation.operation.get().to_le_bytes());
        bytes.extend(self.observation.index.to_le_bytes());
        bytes.extend((update.len() as u32).to_le_bytes());
        bytes.extend(update);
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, ApplicationError> {
        if bytes.len() > MAX_METADATA_LOCATOR_ADOPTION_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBMLAD01" {
            return Err(ApplicationError::InvalidCommand);
        }
        let configuration =
            ConfigurationId::new(r.u64()?).ok_or(ApplicationError::InvalidCommand)?;
        let operation = r.operation()?;
        let index = r.u64()?;
        let n = r.u32()? as usize;
        let encoded = r.take(n)?;
        let update = MetadataLocatorUpdate::decode(encoded)?;
        if !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        let observation = MetadataLocatorStatus {
            operation,
            index,
            authority: update.before().input().authority,
            responsibility: update.before().input().responsibility,
            generation: update.after().input().generation,
            command_digest: ContentDigest::sha256(encoded),
            activation: update.activation(),
        };
        Self::new(update, configuration, observation)
    }
}
impl<A: CheckpointStateMachine, P: PartitionPolicy + Clone> RoutedApplication<A, P> {
    /// Select before bootstrap; shares the metadata/parent history reserve.
    #[allow(clippy::result_large_err)]
    pub fn with_metadata_locator_adoption(
        self,
        maximum: usize,
    ) -> Result<Self, (ApplicationError, Self)> {
        let mut next = self.with_metadata_authority_adoption(maximum)?;
        next.metadata_locator_adoption = true;
        next.binding[..8].copy_from_slice(b"VBROWN06");
        Ok(next)
    }
    pub fn metadata_locator_adoption(
        &self,
        operation: OperationId,
    ) -> Option<MetadataLocatorGrantStatus> {
        self.parent_adoptions.iter().find_map(|r| match &r.command {
            ParentAdoptionCommand::Locator(c) if r.status.operation == operation => {
                Some(c.status(r.status))
            }
            _ => None,
        })
    }
}
