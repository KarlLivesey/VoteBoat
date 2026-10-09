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
//! Bounded ancestry/subtree guards for cross-authority reparent decisions.
//! Quorum observations are caller authenticated; encodings are not certificates.
use crate::{
    application::*,
    identity::*,
    routing::{codec::*, *},
    transfer::ContentDigest,
};
use std::{collections::BTreeSet, mem::size_of};
pub const REPARENT_GUARD_DIRECTORY_SCHEMA: u64 = 12;
pub const MAX_REPARENT_GUARD_MANIFESTS: usize = 128;
pub const MAX_REPARENT_GUARD_BYTES: usize =
    82 + MAX_REPARENT_GUARD_MANIFESTS * (4 + MAX_MANIFEST_BYTES);
pub const MAX_REPARENT_COMPLETION_BYTES: usize = 16 * 1024;
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CrossReparentPlan {
    old_parent: ResponsibilityIdentity,
    new_parent: ResponsibilityIdentity,
    child: ResponsibilityIdentity,
    manifests: Vec<ResponsibilityManifest>,
}
impl CrossReparentPlan {
    pub fn new(
        old_parent: ResponsibilityIdentity,
        new_parent: ResponsibilityIdentity,
        child: ResponsibilityIdentity,
        mut manifests: Vec<ResponsibilityManifest>,
    ) -> Result<Self, ApplicationError> {
        if manifests.is_empty()
            || manifests.len() > MAX_REPARENT_GUARD_MANIFESTS
            || manifests.capacity() > MAX_REPARENT_GUARD_MANIFESTS
        {
            return Err(ApplicationError::InvalidCommand);
        }
        manifests.sort_by_key(|m| m.input().responsibility);
        let p = Self {
            old_parent,
            new_parent,
            child,
            manifests,
        };
        p.validate()?;
        Ok(p)
    }
    pub fn manifests(&self) -> &[ResponsibilityManifest] {
        &self.manifests
    }
    fn lookup(
        &self,
        id: ResponsibilityIdentity,
    ) -> Result<&ResponsibilityManifest, ApplicationError> {
        self.manifests
            .iter()
            .find(|m| m.input().responsibility == id)
            .ok_or(ApplicationError::InvalidCommand)
    }
    pub fn old_parent(&self) -> &ResponsibilityManifest {
        self.lookup(self.old_parent).expect("checked old parent")
    }
    pub fn new_parent(&self) -> &ResponsibilityManifest {
        self.lookup(self.new_parent).expect("checked new parent")
    }
    pub fn child(&self) -> &ResponsibilityManifest {
        self.lookup(self.child).expect("checked child")
    }
    pub fn authorities(&self) -> Vec<GroupIdentity> {
        self.manifests
            .iter()
            .map(|m| m.input().authority)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }
    pub fn coordinator(&self) -> GroupIdentity {
        self.manifests
            .iter()
            .map(|m| m.input().authority)
            .min()
            .expect("nonempty plan")
    }
    pub fn retained_bytes(&self) -> usize {
        size_of::<Self>()
            + self
                .manifests
                .iter()
                .map(ResponsibilityManifest::retained_bytes)
                .sum::<usize>()
            + (self.manifests.capacity() - self.manifests.len())
                * size_of::<ResponsibilityManifest>()
    }
    fn delegates(parent: &ResponsibilityManifest, child: &ResponsibilityManifest) -> bool {
        let (p, c) = (parent.input(), child.input());
        c.parent
            == Some(ParentAuthority {
                responsibility: p.responsibility,
                group: p.authority,
            })
            && matches!(&p.execution,ExecutionMode::Delegated(routes) if routes.iter().any(|r|r.scope==c.scope && r.target==RouteTarget::Child(ChildAuthority{responsibility:c.responsibility,group:c.authority,epoch:c.epoch})))
    }
    fn validate(&self) -> Result<(), ApplicationError> {
        let invalid = ApplicationError::InvalidCommand;
        if self.manifests.is_empty()
            || self.manifests.len() > MAX_REPARENT_GUARD_MANIFESTS
            || self.old_parent == self.new_parent
            || self.old_parent == self.child
            || self.new_parent == self.child
        {
            return Err(invalid);
        }
        let old = self.lookup(self.old_parent)?;
        let new = self.lookup(self.new_parent)?;
        let child = self.lookup(self.child)?;
        for (i, m) in self.manifests.iter().enumerate() {
            let b = m.input();
            if b.state != ResponsibilityState::Active
                || b.scheme != child.input().scheme
                || b.application != child.input().application
                || (i > 0 && self.manifests[i - 1].input().responsibility >= b.responsibility)
                || self.manifests[..i]
                    .iter()
                    .any(|n| n.input().responsibility.id == b.responsibility.id)
            {
                return Err(invalid);
            }
        }
        if [old, new, child]
            .iter()
            .any(|m| m.input().generation.get() == u64::MAX)
            || !Self::delegates(old, child)
            || !matches!(&new.input().execution,ExecutionMode::Delegated(routes) if routes.iter().any(|r|r.scope==child.input().scope && r.target==RouteTarget::Vacant))
        {
            return Err(invalid);
        }
        let mut used = BTreeSet::from([self.old_parent]);
        let mut ancestry = BTreeSet::new();
        let mut current = new;
        loop {
            let id = current.input().responsibility;
            if id == self.child || !ancestry.insert(id) || ancestry.len() >= MAX_ROUTE_HOPS {
                return Err(invalid);
            }
            used.insert(id);
            let Some(p) = current.input().parent else {
                break;
            };
            let parent = self.lookup(p.responsibility)?;
            if parent.input().authority != p.group || !Self::delegates(parent, current) {
                return Err(invalid);
            }
            current = parent;
        }
        let mut subtree = BTreeSet::new();
        let mut stack = vec![(self.child, 1usize)];
        while let Some((id, depth)) = stack.pop() {
            if ancestry.contains(&id)
                || !subtree.insert(id)
                || ancestry.len() + depth > MAX_ROUTE_HOPS
            {
                return Err(invalid);
            }
            used.insert(id);
            let m = self.lookup(id)?;
            if let ExecutionMode::Delegated(routes) = &m.input().execution {
                for r in routes {
                    if let RouteTarget::Child(c) = r.target {
                        let desc = self.lookup(c.responsibility)?;
                        if desc.input().authority != c.group || !Self::delegates(m, desc) {
                            return Err(invalid);
                        }
                        stack.push((c.responsibility, depth + 1));
                    }
                }
            }
        }
        if used.len() != self.manifests.len() {
            return Err(invalid);
        }
        Ok(())
    }
    pub fn encode(&self, max: usize) -> Result<Vec<u8>, ApplicationError> {
        let len = 82
            + self
                .manifests
                .iter()
                .map(|m| 4 + manifest_len(m))
                .sum::<usize>();
        if len > max.min(MAX_REPARENT_GUARD_BYTES) {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut b = Vec::with_capacity(len);
        b.extend(b"VBXRPL01");
        for id in [self.old_parent, self.new_parent, self.child] {
            put_responsibility(&mut b, id);
        }
        b.extend((self.manifests.len() as u16).to_le_bytes());
        for m in &self.manifests {
            b.extend((manifest_len(m) as u32).to_le_bytes());
            put_manifest(&mut b, m);
        }
        Ok(b)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, ApplicationError> {
        if bytes.len() > MAX_REPARENT_GUARD_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBXRPL01" {
            return Err(ApplicationError::InvalidCommand);
        }
        let old_parent = r.responsibility()?;
        let new_parent = r.responsibility()?;
        let child = r.responsibility()?;
        let n = r.u16()? as usize;
        if n == 0 || n > MAX_REPARENT_GUARD_MANIFESTS {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut manifests = Vec::with_capacity(n);
        for _ in 0..n {
            let len = r.u32()? as usize;
            manifests.push(read_manifest(r.take(len)?)?);
        }
        let p = Self {
            old_parent,
            new_parent,
            child,
            manifests,
        };
        if !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        p.validate()?;
        Ok(p)
    }
    /// Exact old-parent, new-parent and child manifests after this move.
    pub fn updated_manifests(&self) -> [ResponsibilityManifest; 3] {
        let mut old = self.old_parent().clone().into_input();
        let mut new = self.new_parent().clone().into_input();
        let mut child = self.child().clone().into_input();
        for (m, target) in [
            (&mut old, RouteTarget::Vacant),
            (
                &mut new,
                RouteTarget::Child(ChildAuthority {
                    responsibility: child.responsibility,
                    group: child.authority,
                    epoch: child.epoch,
                }),
            ),
        ] {
            let ExecutionMode::Delegated(routes) = &mut m.execution else {
                unreachable!("checked plan")
            };
            routes
                .iter_mut()
                .find(|r| r.scope == child.scope)
                .expect("checked selector")
                .target = target;
            m.generation = RouteGeneration::new(m.generation.get() + 1).unwrap();
        }
        child.parent = Some(ParentAuthority {
            responsibility: new.responsibility,
            group: new.authority,
        });
        child.generation = RouteGeneration::new(child.generation.get() + 1).unwrap();
        [old, new, child].map(|m| ResponsibilityManifest::new(m).expect("checked reparent shape"))
    }
    pub fn digest(&self) -> ContentDigest {
        ContentDigest::sha256(
            &self
                .encode(MAX_REPARENT_GUARD_BYTES)
                .expect("checked bounded plan"),
        )
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReparentGuardStatus {
    pub authority: GroupIdentity,
    pub operation: OperationId,
    pub index: u64,
    pub plan: CrossReparentPlan,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CancelReparent {
    pub guard: OperationId,
}
impl CancelReparent {
    pub fn encode(self) -> Vec<u8> {
        let mut b = b"VBXRCN01".to_vec();
        b.extend(self.guard.get().to_le_bytes());
        b
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, ApplicationError> {
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBXRCN01" {
            return Err(ApplicationError::InvalidCommand);
        }
        let guard = r.operation()?;
        if !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(Self { guard })
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReparentCancellationStatus {
    pub coordinator: GroupIdentity,
    pub guard: OperationId,
    pub guard_index: u64,
    pub digest: ContentDigest,
    pub operation: OperationId,
    pub index: u64,
}
/// Original coordinator cancellation observed through its authenticated quorum.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReleaseReparentGuard {
    pub configuration: ConfigurationId,
    pub decision: ReparentCancellationStatus,
}
impl ReleaseReparentGuard {
    pub fn encode(self) -> Result<Vec<u8>, ApplicationError> {
        let s = self.decision;
        if s.guard_index == 0
            || s.index <= s.guard_index
            || s.index == u64::MAX
            || s.operation == s.guard
        {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut b = b"VBXRAB01".to_vec();
        b.extend(self.configuration.get().to_le_bytes());
        put_group(&mut b, s.coordinator);
        b.extend(s.guard.get().to_le_bytes());
        b.extend(s.guard_index.to_le_bytes());
        b.extend(s.digest.0);
        b.extend(s.operation.get().to_le_bytes());
        b.extend(s.index.to_le_bytes());
        Ok(b)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, ApplicationError> {
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBXRAB01" {
            return Err(ApplicationError::InvalidCommand);
        }
        let configuration =
            ConfigurationId::new(r.u64()?).ok_or(ApplicationError::InvalidCommand)?;
        let decision = ReparentCancellationStatus {
            coordinator: r.group()?,
            guard: r.operation()?,
            guard_index: r.u64()?,
            digest: ContentDigest(r.take(32)?.try_into().unwrap()),
            operation: r.operation()?,
            index: r.u64()?,
        };
        let c = Self {
            configuration,
            decision,
        };
        if !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        c.encode()?;
        Ok(c)
    }
}

/// A fixed original guard observation; foreign provenance is verified by the host.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReparentGuardEvidence {
    pub authority: GroupIdentity,
    pub configuration: ConfigurationId,
    pub operation: OperationId,
    pub index: u64,
    pub digest: ContentDigest,
}
impl ReparentGuardEvidence {
    pub fn from_status(
        configuration: ConfigurationId,
        status: &ReparentGuardStatus,
    ) -> Result<Self, ApplicationError> {
        if status.index == 0
            || status.index == u64::MAX
            || !status.plan.authorities().contains(&status.authority)
        {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(Self {
            authority: status.authority,
            configuration,
            operation: status.operation,
            index: status.index,
            digest: status.plan.digest(),
        })
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrepareReparent {
    pub plan: CrossReparentPlan,
    pub coordinator: Option<ReparentGuardEvidence>,
}
pub const MAX_REPARENT_PREPARE_BYTES: usize = MAX_REPARENT_GUARD_BYTES + 101;
impl PrepareReparent {
    pub fn encode(&self, max: usize) -> Result<Vec<u8>, ApplicationError> {
        let p = self.plan.encode(MAX_REPARENT_GUARD_BYTES)?;
        let len = 13 + p.len() + self.coordinator.map_or(0, |_| 88);
        if len > max.min(MAX_REPARENT_PREPARE_BYTES) {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut b = b"VBXRGR01".to_vec();
        b.extend((p.len() as u32).to_le_bytes());
        b.extend(p);
        b.push(u8::from(self.coordinator.is_some()));
        if let Some(c) = self.coordinator {
            if c.index == 0
                || c.index == u64::MAX
                || c.authority != self.plan.coordinator()
                || c.digest != self.plan.digest()
            {
                return Err(ApplicationError::InvalidCommand);
            }
            put_group(&mut b, c.authority);
            b.extend(c.configuration.get().to_le_bytes());
            b.extend(c.operation.get().to_le_bytes());
            b.extend(c.index.to_le_bytes());
            b.extend(c.digest.0);
        }
        Ok(b)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, ApplicationError> {
        if bytes.len() > MAX_REPARENT_PREPARE_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBXRGR01" {
            return Err(ApplicationError::InvalidCommand);
        }
        let n = r.u32()? as usize;
        let plan = CrossReparentPlan::decode(r.take(n)?)?;
        let coordinator = if r.boolean()? {
            Some(ReparentGuardEvidence {
                authority: r.group()?,
                configuration: ConfigurationId::new(r.u64()?)
                    .ok_or(ApplicationError::InvalidCommand)?,
                operation: r.operation()?,
                index: r.u64()?,
                digest: ContentDigest(r.take(32)?.try_into().unwrap()),
            })
        } else {
            None
        };
        if !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        let request = Self { plan, coordinator };
        request.encode(MAX_REPARENT_PREPARE_BYTES)?;
        Ok(request)
    }
}
