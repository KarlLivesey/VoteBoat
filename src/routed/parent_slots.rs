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
//! Checked parent child-slot changes; observations require original-quorum authentication.
//! Shared control codecs; only the scoped-source schema5 profile admits them.
use super::*;
use crate::reparent_commit::ReparentPublicationStatus;
use crate::transfer::ContentDigest;

pub const PARENT_SLOT_SCOPED_TRANSFER_SOURCE_SCHEMA: u64 = 5;
pub const MAX_PARENT_SLOT_ADOPTION_BYTES: usize = 117 + MAX_CROSS_PARENT_ADOPTION_BYTES;
const _: () = assert!(MAX_PARENT_SLOT_ADOPTION_BYTES <= MAX_ROUTED_COMMAND_BYTES);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReparentSide {
    Old,
    New,
}
impl ReparentSide {
    fn position(self) -> usize {
        match self {
            Self::Old => 0,
            Self::New => 1,
        }
    }
    fn read(r: &mut Reader<'_>) -> Result<Self, ApplicationError> {
        match r.take(1)?[0] {
            0 => Ok(Self::Old),
            1 => Ok(Self::New),
            _ => Err(ApplicationError::InvalidCommand),
        }
    }
}
/// A local atomic reparent decision, authenticated by the metadata quorum.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParentSlotAdoption {
    pub side: ReparentSide,
    pub observation: OwnerParentAdoption,
}
impl ParentSlotAdoption {
    pub fn before(&self) -> &ResponsibilityManifest {
        match self.side {
            ReparentSide::Old => self.observation.decision.plan.old_parent(),
            ReparentSide::New => self.observation.decision.plan.new_parent(),
        }
    }
    pub fn after(&self) -> ResponsibilityManifest {
        self.observation.decision.plan.updated_manifests()[self.side.position()].clone()
    }
    pub fn encode(&self, max: usize) -> Result<Vec<u8>, ApplicationError> {
        wrap(
            b"VBSLAD01",
            self.side,
            &[],
            self.observation.encode(MAX_PARENT_ADOPTION_BYTES)?,
            max,
        )
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, ApplicationError> {
        let mut r = reader(bytes, b"VBSLAD01")?;
        let side = ReparentSide::read(&mut r)?;
        let n = r.u32()? as usize;
        let observation = OwnerParentAdoption::decode(r.take(n)?)?;
        if !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(Self { side, observation })
    }
}
/// Completed cross-authority move plus the selected parent's original publication.
/// The host authenticates all observations, including this parent configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CrossParentSlotAdoption {
    pub side: ReparentSide,
    pub observation: CrossOwnerParentAdoption,
    pub parent_configuration: ConfigurationId,
    pub parent_publication: ReparentPublicationStatus,
}
impl CrossParentSlotAdoption {
    pub fn before(&self) -> &ResponsibilityManifest {
        match self.side {
            ReparentSide::Old => self.observation.plan.old_parent(),
            ReparentSide::New => self.observation.plan.new_parent(),
        }
    }
    pub fn after(&self) -> ResponsibilityManifest {
        self.observation.plan.updated_manifests()[self.side.position()].clone()
    }
    fn validate(&self) -> Result<(), ApplicationError> {
        let d = &self.observation.decision.decision;
        let p = self.parent_publication;
        if p.authority != self.before().input().authority
            || p.guard != d.commit.guard
            || p.decision_digest != d.digest()?
            || p.operation == p.guard
            || p.index == u64::MAX
            || !d
                .commit
                .guards
                .iter()
                .any(|g| g.authority == p.authority && p.index > g.index)
            || (p.authority == d.coordinator
                && (p.operation != d.operation
                    || p.index != d.index
                    || self.parent_configuration != self.observation.decision.configuration))
            || (p.authority == self.observation.child_publication.authority
                && (p != self.observation.child_publication
                    || self.parent_configuration != self.observation.child_configuration))
        {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(())
    }
    pub fn encode(&self, max: usize) -> Result<Vec<u8>, ApplicationError> {
        self.validate()?;
        let p = self.parent_publication;
        let mut extra = Vec::with_capacity(104);
        extra.extend(self.parent_configuration.get().to_le_bytes());
        extra.extend(p.authority.id.get().to_le_bytes());
        extra.extend(p.authority.incarnation.get().to_le_bytes());
        extra.extend(p.operation.get().to_le_bytes());
        extra.extend(p.index.to_le_bytes());
        extra.extend(p.guard.get().to_le_bytes());
        extra.extend(p.decision_digest.0);
        wrap(
            b"VBSXAD01",
            self.side,
            &extra,
            self.observation.encode(MAX_CROSS_PARENT_ADOPTION_BYTES)?,
            max,
        )
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, ApplicationError> {
        let mut r = reader(bytes, b"VBSXAD01")?;
        let side = ReparentSide::read(&mut r)?;
        let parent_configuration =
            ConfigurationId::new(r.u64()?).ok_or(ApplicationError::InvalidCommand)?;
        let parent_publication = ReparentPublicationStatus {
            authority: r.group()?,
            operation: r.operation()?,
            index: r.u64()?,
            guard: r.operation()?,
            decision_digest: ContentDigest(r.take(32)?.try_into().unwrap()),
        };
        let n = r.u32()? as usize;
        let observation = CrossOwnerParentAdoption::decode(r.take(n)?)?;
        if !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        let value = Self {
            side,
            observation,
            parent_configuration,
            parent_publication,
        };
        value.validate()?;
        Ok(value)
    }
}
fn reader<'a>(bytes: &'a [u8], tag: &[u8; 8]) -> Result<Reader<'a>, ApplicationError> {
    if bytes.len() > MAX_PARENT_SLOT_ADOPTION_BYTES {
        return Err(ApplicationError::InvalidCommand);
    }
    let mut r = Reader::new(bytes);
    if r.take(8)? != tag {
        return Err(ApplicationError::InvalidCommand);
    }
    Ok(r)
}
fn wrap(
    tag: &[u8; 8],
    side: ReparentSide,
    extra: &[u8],
    body: Vec<u8>,
    max: usize,
) -> Result<Vec<u8>, ApplicationError> {
    let size = 13 + extra.len() + body.len();
    if size > max.min(MAX_PARENT_SLOT_ADOPTION_BYTES) {
        return Err(ApplicationError::InvalidCommand);
    }
    let mut out = Vec::with_capacity(size);
    out.extend(tag);
    out.push(side.position() as u8);
    out.extend(extra);
    out.extend((body.len() as u32).to_le_bytes());
    out.extend(body);
    Ok(out)
}
