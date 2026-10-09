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
//! Fresh independent namespace creation. No existing selector is reassigned.
mod application;
use crate::{
    application::*,
    directory::*,
    identity::*,
    routing::{codec::*, *},
    transfer::ContentDigest,
};
pub use application::*;

pub const MAX_NAMESPACE_PLAN_BYTES: usize = MAX_GROUP_CREATION_BYTES + MAX_MANIFEST_BYTES + 48;
pub const MAX_NAMESPACE_PUBLICATION_BYTES: usize = 1024;
/// The creation anchor authorizes administration; it is not a routing-parent
/// edge. This initial path creates a distinct root namespace, never steals scope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespacePlan {
    pub creation: GroupCreationStatus,
    pub manifest: ResponsibilityManifest,
}
impl NamespacePlan {
    pub fn validate(&self) -> Result<(), ApplicationError> {
        let c = &self.creation;
        let i = &c.intent;
        let m = self.manifest.input();
        i.encode(MAX_GROUP_CREATION_BYTES)?;
        if c.index == 0
            || i.mode != GroupCreationMode::Empty
            || m.parent.is_some()
            || m.authority != i.authority
            || m.responsibility != i.responsibility
            || m.application != i.application
            || m.epoch.get() != 1
            || m.generation.get() != 1
            || m.state != ResponsibilityState::Active
            || m.execution != ExecutionMode::Single(i.bootstrap.group)
            || m.placement.minimum_voting_domains > i.bootstrap.voter_stores.len()
        {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(())
    }
    pub fn encode(&self, max: usize) -> Result<Vec<u8>, ApplicationError> {
        self.validate()?;
        let intent = self.creation.intent.encode(MAX_GROUP_CREATION_BYTES)?;
        let len = 40 + intent.len() + manifest_len(&self.manifest);
        if len > max.min(MAX_NAMESPACE_PLAN_BYTES) {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut b = Vec::with_capacity(len);
        b.extend(b"VBNSPLN1");
        b.extend(self.creation.operation.get().to_le_bytes());
        b.extend(self.creation.index.to_le_bytes());
        b.extend((intent.len() as u32).to_le_bytes());
        b.extend(intent);
        b.extend((manifest_len(&self.manifest) as u32).to_le_bytes());
        put_manifest(&mut b, &self.manifest);
        Ok(b)
    }
    pub fn decode(b: &[u8]) -> Result<Self, ApplicationError> {
        if b.len() > MAX_NAMESPACE_PLAN_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut r = Reader::new(b);
        if r.take(8)? != b"VBNSPLN1" {
            return Err(ApplicationError::InvalidCommand);
        }
        let operation = r.operation()?;
        let index = r.u64()?;
        let len = r.u32()? as usize;
        let intent = GroupCreationIntent::decode(r.take(len)?)?;
        let len = r.u32()? as usize;
        let manifest = read_manifest(r.take(len)?)?;
        if !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        let p = Self {
            creation: GroupCreationStatus {
                operation,
                index,
                intent,
            },
            manifest,
        };
        p.validate()?;
        Ok(p)
    }
    pub fn digest(&self) -> Result<ContentDigest, ApplicationError> {
        Ok(ContentDigest::sha256(
            &self.encode(MAX_NAMESPACE_PLAN_BYTES)?,
        ))
    }
}
/// Original applied target facts. Authenticate actual target commitment before
/// using remotely. Digest equality is structural binding, not authentication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NamespaceStatus {
    pub plan: ContentDigest,
    pub ready_index: Option<u64>,
    pub activation_index: Option<u64>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespacePublication {
    pub creation: OperationId,
    pub creation_index: u64,
    pub manifest: ResponsibilityManifest,
    pub configuration: ConfigurationId,
    pub ready_index: u64,
    pub plan: ContentDigest,
}
impl NamespacePublication {
    pub fn from_status(
        plan: &NamespacePlan,
        status: NamespaceStatus,
    ) -> Result<Self, ApplicationError> {
        plan.validate()?;
        if status.plan != plan.digest()? || status.ready_index.is_none_or(|i| i == 0) {
            return Err(ApplicationError::NotApplied);
        }
        Ok(Self {
            creation: plan.creation.operation,
            creation_index: plan.creation.index,
            manifest: plan.manifest.clone(),
            configuration: plan.creation.intent.bootstrap.configuration,
            ready_index: status.ready_index.unwrap(),
            plan: status.plan,
        })
    }
    pub fn matches(&self, p: &NamespacePlan) -> Result<(), ApplicationError> {
        p.validate()?;
        if self.creation != p.creation.operation
            || self.creation_index != p.creation.index
            || self.manifest != p.manifest
            || self.configuration != p.creation.intent.bootstrap.configuration
            || self.ready_index == 0
            || self.plan != p.digest()?
        {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(())
    }
    pub fn encode(&self, max: usize) -> Result<Vec<u8>, ApplicationError> {
        let len = 80 + manifest_len(&self.manifest);
        if self.creation_index == 0
            || self.ready_index == 0
            || len > max.min(MAX_NAMESPACE_PUBLICATION_BYTES)
        {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut b = Vec::with_capacity(len);
        b.extend(b"VBNSPUB1");
        b.extend(self.creation.get().to_le_bytes());
        b.extend(self.creation_index.to_le_bytes());
        b.extend(self.configuration.get().to_le_bytes());
        b.extend(self.ready_index.to_le_bytes());
        b.extend(self.plan.0);
        put_manifest(&mut b, &self.manifest);
        Ok(b)
    }
    pub fn decode(b: &[u8]) -> Result<Self, ApplicationError> {
        if b.len() > MAX_NAMESPACE_PUBLICATION_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut r = Reader::new(b);
        if r.take(8)? != b"VBNSPUB1" {
            return Err(ApplicationError::InvalidCommand);
        }
        let creation = r.operation()?;
        let creation_index = r.u64()?;
        let configuration =
            ConfigurationId::new(r.u64()?).ok_or(ApplicationError::InvalidCommand)?;
        let ready_index = r.u64()?;
        let plan = ContentDigest(r.take(32)?.try_into().unwrap());
        // Manifest is the bounded remainder, with its own canonical framing.
        let manifest = read_manifest(r.take(b.len() - 80)?)?;
        if !r.done() || creation_index == 0 || ready_index == 0 {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(Self {
            creation,
            creation_index,
            manifest,
            configuration,
            ready_index,
            plan,
        })
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespacePublicationStatus {
    pub operation: OperationId,
    pub index: u64,
    pub publication: NamespacePublication,
}
