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
//! Forward-only recursive deletion observations. Values bind content and shape;
//! the host must authenticate original quorum observations before proposal.
use crate::{
    application::*,
    identity::*,
    routed::OwnershipFence,
    routing::{codec::*, *},
    transfer::ContentDigest,
};
use std::{collections::BTreeSet, mem::size_of};

pub const DELETION_DIRECTORY_SCHEMA: u64 = 9;
pub const MAX_DELETION_COMPLETION_BYTES: usize = 128 * 1024;
pub const MAX_DELETION_INTENT_BYTES: usize = 32768;
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeletionIntent {
    pub before: ResponsibilityManifest,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeletionIntentStatus {
    pub operation: OperationId,
    pub index: u64,
    pub intent: DeletionIntent,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeletionStatus {
    pub operation: OperationId,
    pub index: u64,
    pub intent: DeletionIntentStatus,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeletionFenceEvidence {
    pub configuration: ConfigurationId,
    pub fence: OwnershipFence,
}
/// Fixed projection of an authenticated original child deletion read. It is not
/// a certificate; the host must verify its authority/configuration and commitment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChildDeletionEvidence {
    pub configuration: ConfigurationId,
    pub authority: GroupIdentity,
    pub responsibility: ResponsibilityIdentity,
    pub parent: ParentAuthority,
    pub scope: BucketRange,
    pub epoch: OwnershipEpoch,
    pub generation: RouteGeneration,
    pub intent: OperationId,
    pub intent_index: u64,
    pub operation: OperationId,
    pub index: u64,
    pub digest: ContentDigest,
}
impl ChildDeletionEvidence {
    pub(crate) fn put(&self, b: &mut Vec<u8>) {
        b.extend(self.configuration.get().to_le_bytes());
        put_group(b, self.authority);
        put_responsibility(b, self.responsibility);
        put_responsibility(b, self.parent.responsibility);
        put_group(b, self.parent.group);
        put_range(b, self.scope);
        b.extend(self.epoch.get().to_le_bytes());
        b.extend(self.generation.get().to_le_bytes());
        b.extend(self.intent.get().to_le_bytes());
        b.extend(self.intent_index.to_le_bytes());
        b.extend(self.operation.get().to_le_bytes());
        b.extend(self.index.to_le_bytes());
        b.extend(self.digest.0);
    }
    pub(crate) fn read(r: &mut Reader<'_>) -> Result<Self, ApplicationError> {
        let value = Self {
            configuration: ConfigurationId::new(r.u64()?)
                .ok_or(ApplicationError::InvalidCommand)?,
            authority: r.group()?,
            responsibility: r.responsibility()?,
            parent: ParentAuthority {
                responsibility: r.responsibility()?,
                group: r.group()?,
            },
            scope: r.range()?,
            epoch: OwnershipEpoch::new(r.u64()?).ok_or(ApplicationError::InvalidCommand)?,
            generation: RouteGeneration::new(r.u64()?).ok_or(ApplicationError::InvalidCommand)?,
            intent: r.operation()?,
            intent_index: r.u64()?,
            operation: r.operation()?,
            index: r.u64()?,
            digest: ContentDigest(r.take(32)?.try_into().unwrap()),
        };
        value.validate()?;
        Ok(value)
    }
    pub fn from_status(
        configuration: ConfigurationId,
        s: &DeletionStatus,
    ) -> Result<Self, ApplicationError> {
        s.validate()?;
        let m = s.intent.intent.before.input();
        Ok(Self {
            configuration,
            authority: m.authority,
            responsibility: m.responsibility,
            parent: m.parent.ok_or(ApplicationError::InvalidCommand)?,
            scope: m.scope,
            epoch: m.epoch,
            generation: m.generation,
            intent: s.intent.operation,
            intent_index: s.intent.index,
            operation: s.operation,
            index: s.index,
            digest: ContentDigest::sha256(&s.intent.intent.encode(MAX_DELETION_INTENT_BYTES)?),
        })
    }
    pub(crate) fn validate(&self) -> Result<(), ApplicationError> {
        if !index(self.intent_index)
            || !index(self.index)
            || self.index <= self.intent_index
            || self.operation == self.intent
            || self.responsibility == self.parent.responsibility
        {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeletionCompletion {
    pub intent: DeletionIntentStatus,
    pub fences: Vec<DeletionFenceEvidence>,
    pub children: Vec<ChildDeletionEvidence>,
}
fn index(i: u64) -> bool {
    i != 0 && i != u64::MAX
}
impl DeletionIntent {
    fn validate(&self) -> Result<(), ApplicationError> {
        let m = self.before.input();
        if m.state != ResponsibilityState::Active || m.generation.get() == u64::MAX {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(())
    }
    pub fn encode(&self, max: usize) -> Result<Vec<u8>, ApplicationError> {
        self.validate()?;
        let len = 12 + manifest_len(&self.before);
        if len > max.min(MAX_DELETION_INTENT_BYTES) {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut b = Vec::with_capacity(len);
        b.extend(b"VBDDEL01");
        b.extend((manifest_len(&self.before) as u32).to_le_bytes());
        put_manifest(&mut b, &self.before);
        Ok(b)
    }
    pub fn decode(b: &[u8]) -> Result<Self, ApplicationError> {
        if b.len() > MAX_DELETION_INTENT_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut r = Reader::new(b);
        if r.take(8)? != b"VBDDEL01" {
            return Err(ApplicationError::InvalidCommand);
        }
        let n = r.u32()? as usize;
        let s = Self {
            before: read_manifest(r.take(n)?)?,
        };
        if !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        s.validate()?;
        Ok(s)
    }
    pub fn retained_bytes(&self) -> usize {
        size_of::<Self>() + self.before.retained_bytes() - size_of::<ResponsibilityManifest>()
    }
    pub fn tombstone(&self) -> Result<ResponsibilityManifest, ApplicationError> {
        self.validate()?;
        let mut m = self.before.clone().into_input();
        m.state = ResponsibilityState::Fenced;
        m.generation = RouteGeneration::new(m.generation.get() + 1).unwrap();
        ResponsibilityManifest::new(m).map_err(|_| ApplicationError::InvalidCommand)
    }
}
impl DeletionStatus {
    fn validate(&self) -> Result<(), ApplicationError> {
        self.intent.intent.validate()?;
        if !index(self.intent.index)
            || !index(self.index)
            || self.index <= self.intent.index
            || self.operation == self.intent.operation
        {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(())
    }
}
impl DeletionCompletion {
    pub fn validate(&self) -> Result<(), ApplicationError> {
        self.intent.intent.validate()?;
        if !index(self.intent.index)
            || self.fences.len() > MAX_MANIFEST_ROUTES
            || self.fences.capacity() > MAX_MANIFEST_ROUTES
            || self.children.len() > MAX_MANIFEST_ROUTES
            || self.children.capacity() > MAX_MANIFEST_ROUTES
        {
            return Err(ApplicationError::InvalidCommand);
        }
        let m = self.intent.intent.before.input();
        let mut groups = BTreeSet::new();
        let mut children = Vec::new();
        match &m.execution {
            ExecutionMode::Single(g) => {
                groups.insert(*g);
            }
            ExecutionMode::Partitioned(routes) | ExecutionMode::Delegated(routes) => {
                for r in routes {
                    match r.target {
                        RouteTarget::Vacant => {}
                        RouteTarget::Group(g) => {
                            groups.insert(g);
                        }
                        RouteTarget::Child(c) => {
                            children.push((c.responsibility, c.group, c.epoch, r.scope))
                        }
                    }
                }
            }
        }
        children.sort_by_key(|c| c.0);
        if self.fences.len() != groups.len() || self.children.len() != children.len() {
            return Err(ApplicationError::InvalidCommand);
        }
        for (f, g) in self.fences.iter().zip(groups) {
            if f.fence.group != g
                || f.fence.responsibility != m.responsibility
                || f.fence.epoch != m.epoch
                || f.fence.operation != self.intent.operation
                || !index(f.fence.index)
            {
                return Err(ApplicationError::InvalidCommand);
            }
        }
        for (f, (id, g, e, scope)) in self.children.iter().zip(children) {
            f.validate()?;
            if f.responsibility != id
                || f.authority != g
                || f.epoch != e
                || f.scope != scope
                || f.parent
                    != (ParentAuthority {
                        responsibility: m.responsibility,
                        group: m.authority,
                    })
            {
                return Err(ApplicationError::InvalidCommand);
            }
        }
        Ok(())
    }
    pub fn retained_bytes(&self) -> usize {
        size_of::<Self>() + self.intent.intent.retained_bytes() - size_of::<DeletionIntent>()
            + self.fences.capacity() * size_of::<DeletionFenceEvidence>()
            + self.children.capacity() * size_of::<ChildDeletionEvidence>()
    }
    pub fn encode(&self, max: usize) -> Result<Vec<u8>, ApplicationError> {
        self.validate()?;
        let inner = self.intent.intent.encode(MAX_DELETION_INTENT_BYTES)?;
        let len = 36 + inner.len() + 2 + 88 * self.fences.len() + 2 + 204 * self.children.len();
        if len > max.min(MAX_DELETION_COMPLETION_BYTES) {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut b = Vec::with_capacity(len);
        b.extend(b"VBDDCM01");
        b.extend(self.intent.operation.get().to_le_bytes());
        b.extend(self.intent.index.to_le_bytes());
        b.extend((inner.len() as u32).to_le_bytes());
        b.extend(inner);
        b.extend((self.fences.len() as u16).to_le_bytes());
        for f in &self.fences {
            b.extend(f.configuration.get().to_le_bytes());
            put_group(&mut b, f.fence.group);
            put_responsibility(&mut b, f.fence.responsibility);
            b.extend(f.fence.epoch.get().to_le_bytes());
            b.extend(f.fence.operation.get().to_le_bytes());
            b.extend(f.fence.index.to_le_bytes());
        }
        b.extend((self.children.len() as u16).to_le_bytes());
        for f in &self.children {
            f.put(&mut b);
        }
        if b.len() > max.min(MAX_DELETION_COMPLETION_BYTES) {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(b)
    }
    pub fn decode(b: &[u8]) -> Result<Self, ApplicationError> {
        if b.len() > MAX_DELETION_COMPLETION_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut r = Reader::new(b);
        if r.take(8)? != b"VBDDCM01" {
            return Err(ApplicationError::InvalidCommand);
        }
        let operation = r.operation()?;
        let idx = r.u64()?;
        let n = r.u32()? as usize;
        let intent = DeletionIntentStatus {
            operation,
            index: idx,
            intent: DeletionIntent::decode(r.take(n)?)?,
        };
        let n = usize::from(r.u16()?);
        if n > MAX_MANIFEST_ROUTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut fences = Vec::with_capacity(n);
        for _ in 0..n {
            fences.push(DeletionFenceEvidence {
                configuration: ConfigurationId::new(r.u64()?)
                    .ok_or(ApplicationError::InvalidCommand)?,
                fence: OwnershipFence {
                    group: r.group()?,
                    responsibility: r.responsibility()?,
                    epoch: OwnershipEpoch::new(r.u64()?).ok_or(ApplicationError::InvalidCommand)?,
                    operation: r.operation()?,
                    index: r.u64()?,
                },
            });
        }
        let n = usize::from(r.u16()?);
        if n > MAX_MANIFEST_ROUTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut children = Vec::with_capacity(n);
        for _ in 0..n {
            children.push(ChildDeletionEvidence::read(&mut r)?);
        }
        if !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        let s = Self {
            intent,
            fences,
            children,
        };
        s.validate()?;
        Ok(s)
    }
}
