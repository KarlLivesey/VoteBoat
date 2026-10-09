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
//! Commit/publication/completion observations for guarded cross-authority moves.
//! Foreign observations require original quorum authentication by the host.
use crate::{
    application::*, identity::*, reparent_guard::*, routing::codec::*, transfer::ContentDigest,
};
use std::mem::size_of;
pub const CROSS_REPARENT_DIRECTORY_SCHEMA: u64 = 13;
pub const MAX_REPARENT_COMMIT_BYTES: usize = 26 + 88 * MAX_REPARENT_GUARD_MANIFESTS;
pub const MAX_REPARENT_PUBLISH_BYTES: usize = 68 + MAX_REPARENT_COMMIT_BYTES;
pub const MAX_REPARENT_FINISH_BYTES: usize = 26 + 72 * MAX_REPARENT_GUARD_MANIFESTS;
const _: () = {
    assert!(MAX_REPARENT_COMMIT_BYTES <= MAX_REPARENT_COMPLETION_BYTES);
    assert!(MAX_REPARENT_PUBLISH_BYTES <= MAX_REPARENT_COMPLETION_BYTES);
    assert!(MAX_REPARENT_FINISH_BYTES <= MAX_REPARENT_COMPLETION_BYTES);
};
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitReparent {
    pub guard: OperationId,
    pub guards: Vec<ReparentGuardEvidence>,
}
impl CommitReparent {
    pub fn new(
        guard: OperationId,
        mut guards: Vec<ReparentGuardEvidence>,
    ) -> Result<Self, ApplicationError> {
        guards.sort_by_key(|g| g.authority);
        let c = Self { guard, guards };
        c.validate()?;
        Ok(c)
    }
    fn validate(&self) -> Result<(), ApplicationError> {
        if self.guards.is_empty()
            || self.guards.len() > MAX_REPARENT_GUARD_MANIFESTS
            || self.guards.capacity() > MAX_REPARENT_GUARD_MANIFESTS
            || self.guards.iter().enumerate().any(|(i, g)| {
                g.operation != self.guard
                    || g.index == 0
                    || g.index == u64::MAX
                    || g.digest != self.guards[0].digest
                    || i > 0 && self.guards[i - 1].authority >= g.authority
            })
        {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(())
    }
    pub fn encode(&self, max: usize) -> Result<Vec<u8>, ApplicationError> {
        self.validate()?;
        let len = 26 + 88 * self.guards.len();
        if len > max.min(MAX_REPARENT_COMMIT_BYTES) {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut b = b"VBXRCM01".to_vec();
        b.extend(self.guard.get().to_le_bytes());
        b.extend((self.guards.len() as u16).to_le_bytes());
        for g in &self.guards {
            put_group(&mut b, g.authority);
            b.extend(g.configuration.get().to_le_bytes());
            b.extend(g.operation.get().to_le_bytes());
            b.extend(g.index.to_le_bytes());
            b.extend(g.digest.0);
        }
        Ok(b)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, ApplicationError> {
        if bytes.len() > MAX_REPARENT_COMMIT_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBXRCM01" {
            return Err(ApplicationError::InvalidCommand);
        }
        let guard = r.operation()?;
        let n = r.u16()? as usize;
        if n == 0 || n > MAX_REPARENT_GUARD_MANIFESTS {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut guards = Vec::with_capacity(n);
        for _ in 0..n {
            guards.push(ReparentGuardEvidence {
                authority: r.group()?,
                configuration: ConfigurationId::new(r.u64()?)
                    .ok_or(ApplicationError::InvalidCommand)?,
                operation: r.operation()?,
                index: r.u64()?,
                digest: ContentDigest(r.take(32)?.try_into().unwrap()),
            });
        }
        let c = Self { guard, guards };
        if !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        c.validate()?;
        Ok(c)
    }
    pub fn retained_bytes(&self) -> usize {
        size_of::<Self>() + self.guards.capacity() * size_of::<ReparentGuardEvidence>()
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReparentDecisionStatus {
    pub coordinator: GroupIdentity,
    pub operation: OperationId,
    pub index: u64,
    pub commit: CommitReparent,
}
impl ReparentDecisionStatus {
    pub fn retained_bytes(&self) -> usize {
        size_of::<Self>() + self.commit.retained_bytes() - size_of::<CommitReparent>()
    }
    fn encode(&self) -> Result<Vec<u8>, ApplicationError> {
        let c = self.commit.encode(MAX_REPARENT_COMMIT_BYTES)?;
        if self.coordinator != self.commit.guards[0].authority
            || self.index <= self.commit.guards[0].index
            || self.index == u64::MAX
            || self.operation == self.commit.guard
        {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut b = Vec::with_capacity(52 + c.len());
        put_group(&mut b, self.coordinator);
        b.extend(self.operation.get().to_le_bytes());
        b.extend(self.index.to_le_bytes());
        b.extend((c.len() as u32).to_le_bytes());
        b.extend(c);
        Ok(b)
    }
    fn decode(bytes: &[u8]) -> Result<Self, ApplicationError> {
        let mut r = Reader::new(bytes);
        let coordinator = r.group()?;
        let operation = r.operation()?;
        let index = r.u64()?;
        let n = r.u32()? as usize;
        let commit = CommitReparent::decode(r.take(n)?)?;
        if !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        let s = Self {
            coordinator,
            operation,
            index,
            commit,
        };
        s.encode()?;
        Ok(s)
    }
    pub fn digest(&self) -> Result<ContentDigest, ApplicationError> {
        Ok(ContentDigest::sha256(&self.encode()?))
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishReparent {
    pub configuration: ConfigurationId,
    pub decision: ReparentDecisionStatus,
}
impl PublishReparent {
    pub fn encode(&self, max: usize) -> Result<Vec<u8>, ApplicationError> {
        let d = self.decision.encode()?;
        if 16 + d.len() > max.min(MAX_REPARENT_PUBLISH_BYTES) {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut b = b"VBXRPU01".to_vec();
        b.extend(self.configuration.get().to_le_bytes());
        b.extend(d);
        Ok(b)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, ApplicationError> {
        if bytes.len() > MAX_REPARENT_PUBLISH_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBXRPU01" {
            return Err(ApplicationError::InvalidCommand);
        }
        let configuration =
            ConfigurationId::new(r.u64()?).ok_or(ApplicationError::InvalidCommand)?;
        let decision = ReparentDecisionStatus::decode(r.take(bytes.len() - 16)?)?;
        Ok(Self {
            configuration,
            decision,
        })
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReparentPublicationStatus {
    pub authority: GroupIdentity,
    pub operation: OperationId,
    pub index: u64,
    pub guard: OperationId,
    pub decision_digest: ContentDigest,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReparentPublicationEvidence {
    pub authority: GroupIdentity,
    pub configuration: ConfigurationId,
    pub index: u64,
    pub decision_digest: ContentDigest,
}
impl ReparentPublicationEvidence {
    pub fn from_status(
        configuration: ConfigurationId,
        s: ReparentPublicationStatus,
    ) -> Result<Self, ApplicationError> {
        if s.index == 0 || s.index == u64::MAX {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(Self {
            authority: s.authority,
            configuration,
            index: s.index,
            decision_digest: s.decision_digest,
        })
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FinishReparent {
    pub guard: OperationId,
    pub publications: Vec<ReparentPublicationEvidence>,
}
impl FinishReparent {
    pub fn new(
        guard: OperationId,
        mut publications: Vec<ReparentPublicationEvidence>,
    ) -> Result<Self, ApplicationError> {
        publications.sort_by_key(|p| p.authority);
        let c = Self {
            guard,
            publications,
        };
        c.validate()?;
        Ok(c)
    }
    fn validate(&self) -> Result<(), ApplicationError> {
        if self.publications.is_empty()
            || self.publications.len() > MAX_REPARENT_GUARD_MANIFESTS
            || self.publications.capacity() > MAX_REPARENT_GUARD_MANIFESTS
            || self.publications.iter().enumerate().any(|(i, p)| {
                p.index == 0
                    || p.index == u64::MAX
                    || p.decision_digest != self.publications[0].decision_digest
                    || i > 0 && self.publications[i - 1].authority >= p.authority
            })
        {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(())
    }
    pub fn encode(&self, max: usize) -> Result<Vec<u8>, ApplicationError> {
        self.validate()?;
        let len = 26 + 72 * self.publications.len();
        if len > max.min(MAX_REPARENT_FINISH_BYTES) {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut b = b"VBXRFI01".to_vec();
        b.extend(self.guard.get().to_le_bytes());
        b.extend((self.publications.len() as u16).to_le_bytes());
        for p in &self.publications {
            put_group(&mut b, p.authority);
            b.extend(p.configuration.get().to_le_bytes());
            b.extend(p.index.to_le_bytes());
            b.extend(p.decision_digest.0);
        }
        Ok(b)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, ApplicationError> {
        if bytes.len() > MAX_REPARENT_FINISH_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBXRFI01" {
            return Err(ApplicationError::InvalidCommand);
        }
        let guard = r.operation()?;
        let n = r.u16()? as usize;
        if n == 0 || n > MAX_REPARENT_GUARD_MANIFESTS {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut publications = Vec::with_capacity(n);
        for _ in 0..n {
            publications.push(ReparentPublicationEvidence {
                authority: r.group()?,
                configuration: ConfigurationId::new(r.u64()?)
                    .ok_or(ApplicationError::InvalidCommand)?,
                index: r.u64()?,
                decision_digest: ContentDigest(r.take(32)?.try_into().unwrap()),
            });
        }
        let c = Self {
            guard,
            publications,
        };
        if !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        c.validate()?;
        Ok(c)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReparentCompletionStatus {
    pub coordinator: GroupIdentity,
    pub operation: OperationId,
    pub index: u64,
    pub guard: OperationId,
    pub decision_digest: ContentDigest,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReleaseCommittedReparent {
    pub configuration: ConfigurationId,
    pub completion: ReparentCompletionStatus,
}
impl ReleaseCommittedReparent {
    pub fn encode(self) -> Result<Vec<u8>, ApplicationError> {
        let s = self.completion;
        if s.index == 0 || s.index == u64::MAX || s.operation == s.guard {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut b = b"VBXRRL01".to_vec();
        b.extend(self.configuration.get().to_le_bytes());
        put_group(&mut b, s.coordinator);
        b.extend(s.operation.get().to_le_bytes());
        b.extend(s.index.to_le_bytes());
        b.extend(s.guard.get().to_le_bytes());
        b.extend(s.decision_digest.0);
        Ok(b)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, ApplicationError> {
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBXRRL01" {
            return Err(ApplicationError::InvalidCommand);
        }
        let configuration =
            ConfigurationId::new(r.u64()?).ok_or(ApplicationError::InvalidCommand)?;
        let completion = ReparentCompletionStatus {
            coordinator: r.group()?,
            operation: r.operation()?,
            index: r.u64()?,
            guard: r.operation()?,
            decision_digest: ContentDigest(r.take(32)?.try_into().unwrap()),
        };
        if !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        let c = Self {
            configuration,
            completion,
        };
        c.encode()?;
        Ok(c)
    }
}
