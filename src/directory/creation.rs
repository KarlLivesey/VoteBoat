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
//! Metadata creation reservations. A local applied status is not a bootstrap certificate.
use super::*;
use crate::quorum::{Limits, Policy, Tree, WeightedChild};

pub const MAX_GROUP_CREATION_BYTES: usize = 1024 * 1024;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupCreationMode {
    Empty,
    Staging,
}
/// Immutable metadata-authorized proposal; host authentication/placement remains
/// required before proposal. Neither decoding nor reserving this starts a group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupCreationIntent {
    pub authority: GroupIdentity,
    pub parent: ResponsibilityIdentity,
    pub expected: RouteGeneration,
    pub responsibility: ResponsibilityIdentity,
    pub bootstrap: Bootstrap,
    pub application: ApplicationAdapter,
    pub mode: GroupCreationMode,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupCreationStatus {
    pub operation: OperationId,
    pub index: u64,
    pub intent: GroupCreationIntent,
}
impl GroupCreationIntent {
    fn validate(&self) -> Result<(), ApplicationError> {
        let b = &self.bootstrap;
        Policy::new(b.policy.tree().clone(), Limits::default())
            .map_err(|_| ApplicationError::InvalidCommand)?;
        if !b.voter_stores.keys().eq(b.policy.voters().iter())
            || b.voter_stores.len() > Limits::default().max_voters
            || self.application.version == 0
            || self.parent.id == self.responsibility.id
            || self.authority.id == b.group.id
        {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(())
    }
    pub fn encode(&self, max_bytes: usize) -> Result<Vec<u8>, ApplicationError> {
        self.validate()?;
        fn tree_len(t: &Tree) -> usize {
            match t {
                Tree::Voter(_) => 9,
                Tree::Majority(c) => 5 + c.iter().map(tree_len).sum::<usize>(),
                Tree::Weighted(c) => 5 + c.iter().map(|w| 8 + tree_len(&w.node)).sum::<usize>(),
            }
        }
        let len = 8
            + 24
            + 24
            + 8
            + 24
            + 24
            + 8
            + 16
            + 4
            + 1
            + tree_len(self.bootstrap.policy.tree())
            + 4
            + 32 * self.bootstrap.voter_stores.len();
        if len > max_bytes.min(MAX_GROUP_CREATION_BYTES) {
            return Err(ApplicationError::InvalidCommand);
        }
        fn put_tree(out: &mut Vec<u8>, t: &Tree) {
            match t {
                Tree::Voter(n) => {
                    out.push(0);
                    out.extend(n.get().to_le_bytes());
                }
                Tree::Majority(c) => {
                    out.push(1);
                    out.extend((c.len() as u32).to_le_bytes());
                    for child in c {
                        put_tree(out, child);
                    }
                }
                Tree::Weighted(c) => {
                    out.push(2);
                    out.extend((c.len() as u32).to_le_bytes());
                    for child in c {
                        out.extend(child.weight.to_le_bytes());
                        put_tree(out, &child.node);
                    }
                }
            }
        }
        let mut out = Vec::with_capacity(len);
        out.extend(b"VBGCRT01");
        put_group(&mut out, self.authority);
        put_responsibility(&mut out, self.parent);
        out.extend(self.expected.get().to_le_bytes());
        put_responsibility(&mut out, self.responsibility);
        put_group(&mut out, self.bootstrap.group);
        out.extend(self.bootstrap.configuration.get().to_le_bytes());
        out.extend(self.application.id.get().to_le_bytes());
        out.extend(self.application.version.to_le_bytes());
        out.push(match self.mode {
            GroupCreationMode::Empty => 0,
            GroupCreationMode::Staging => 1,
        });
        put_tree(&mut out, self.bootstrap.policy.tree());
        out.extend((self.bootstrap.voter_stores.len() as u32).to_le_bytes());
        for (node, store) in &self.bootstrap.voter_stores {
            out.extend(node.get().to_le_bytes());
            out.extend(store.id.get().to_le_bytes());
            out.extend(store.incarnation.get().to_le_bytes());
        }
        Ok(out)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, ApplicationError> {
        if bytes.len() > MAX_GROUP_CREATION_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBGCRT01" {
            return Err(ApplicationError::InvalidCommand);
        }
        let authority = r.group()?;
        let parent = r.responsibility()?;
        let expected = RouteGeneration::new(r.u64()?).ok_or(ApplicationError::InvalidCommand)?;
        let responsibility = r.responsibility()?;
        let group = r.group()?;
        let configuration =
            ConfigurationId::new(r.u64()?).ok_or(ApplicationError::InvalidCommand)?;
        let application = ApplicationAdapter {
            id: ApplicationAdapterId::new(r.u128()?).ok_or(ApplicationError::InvalidCommand)?,
            version: r.u32()?,
        };
        let mode = match r.u8()? {
            0 => GroupCreationMode::Empty,
            1 => GroupCreationMode::Staging,
            _ => return Err(ApplicationError::InvalidCommand),
        };
        fn tree(
            r: &mut Reader<'_>,
            depth: usize,
            remaining: &mut usize,
        ) -> Result<Tree, ApplicationError> {
            if depth > Limits::default().max_depth || *remaining == 0 {
                return Err(ApplicationError::InvalidCommand);
            }
            *remaining -= 1;
            let tag = r.u8()?;
            if tag == 0 {
                return Ok(Tree::Voter(
                    NodeId::new(r.u64()?).ok_or(ApplicationError::InvalidCommand)?,
                ));
            }
            if tag != 1 && tag != 2 {
                return Err(ApplicationError::InvalidCommand);
            }
            let count = r.u32()? as usize;
            if count == 0 || count > *remaining {
                return Err(ApplicationError::InvalidCommand);
            }
            if tag == 1 {
                let mut children = Vec::with_capacity(count);
                for _ in 0..count {
                    children.push(tree(r, depth + 1, remaining)?);
                }
                Ok(Tree::Majority(children))
            } else {
                let mut children = Vec::with_capacity(count);
                for _ in 0..count {
                    children.push(WeightedChild {
                        weight: r.u64()?,
                        node: tree(r, depth + 1, remaining)?,
                    });
                }
                Ok(Tree::Weighted(children))
            }
        }
        let policy = Policy::new(
            tree(&mut r, 0, &mut Limits::default().max_tree_nodes)?,
            Limits::default(),
        )
        .map_err(|_| ApplicationError::InvalidCommand)?;
        let count = r.u32()? as usize;
        if count == 0 || count > Limits::default().max_voters {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut voter_stores = BTreeMap::new();
        let mut last = None;
        for _ in 0..count {
            let node = NodeId::new(r.u64()?).ok_or(ApplicationError::InvalidCommand)?;
            if last.is_some_and(|old| old >= node) {
                return Err(ApplicationError::InvalidCommand);
            }
            last = Some(node);
            let store = StoreIdentity {
                id: StoreId::new(r.u128()?).ok_or(ApplicationError::InvalidCommand)?,
                incarnation: StoreIncarnation::new(r.u64()?)
                    .ok_or(ApplicationError::InvalidCommand)?,
            };
            voter_stores.insert(node, store);
        }
        if !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        let intent = Self {
            authority,
            parent,
            expected,
            responsibility,
            bootstrap: Bootstrap {
                group,
                configuration,
                policy,
                voter_stores,
            },
            application,
            mode,
        };
        intent.validate()?;
        Ok(intent)
    }
}
impl Directory {
    pub(super) fn reserve_group_creation(
        &mut self,
        operation: OperationId,
        intent: GroupCreationIntent,
    ) -> DirectoryOutcome {
        if intent.authority != self.plan.authority {
            return DirectoryOutcome::OwnershipChange;
        }
        let Some(parent) = self.manifests.get(&intent.parent) else {
            return DirectoryOutcome::UnknownResponsibility;
        };
        if parent.input().generation != intent.expected {
            return DirectoryOutcome::GenerationMismatch;
        }
        if parent.input().state != ResponsibilityState::Active {
            return DirectoryOutcome::OwnershipChange;
        }
        if self.transfers.contains_key(&intent.parent)
            || self.delegations.contains_key(&intent.parent)
        {
            return DirectoryOutcome::LifecycleBusy;
        }
        let group = intent.bootstrap.group;
        let child = intent.responsibility;
        let used = self
            .plan
            .manifests
            .values()
            .chain(self.manifests.values())
            .any(|manifest| {
                let i = manifest.input();
                if i.authority.id == group.id || i.responsibility.id == child.id {
                    return true;
                }
                match &i.execution {
                    ExecutionMode::Single(g) => g.id == group.id,
                    ExecutionMode::Partitioned(entries) | ExecutionMode::Delegated(entries) => {
                        entries.iter().any(|e| match e.target {
                            RouteTarget::Group(g) => g.id == group.id,
                            RouteTarget::Child(c) => {
                                c.group.id == group.id || c.responsibility.id == child.id
                            }
                        })
                    }
                }
            });
        if used
            || self.transfer_targets.iter().any(|g| g.id == group.id)
            || self
                .creations
                .iter()
                .any(|(g, (responsibility, _))| g.id == group.id || responsibility.id == child.id)
        {
            return DirectoryOutcome::CreationConflict;
        }
        if self.namespace_creation
            && self.reserved_publication_bytes() + MAX_NAMESPACE_PUBLICATION_BYTES
                > self.control_history_capacity()
        {
            return DirectoryOutcome::TransferControlBusy;
        }
        self.creations.insert(group, (child, operation));
        DirectoryOutcome::CreationReserved
    }
    /// Applied local status only. Establish authenticated metadata quorum read
    /// authority separately; this constructible value is not a remote certificate.
    pub fn group_creation_at(
        &self,
        required: u64,
        group: GroupIdentity,
    ) -> Result<Option<GroupCreationStatus>, ApplicationError> {
        if !self.group_creation {
            return Err(ApplicationError::UnsupportedSchema);
        }
        if required > self.applied {
            return Err(ApplicationError::NotApplied);
        }
        let Some((operation, h)) = self
            .creations
            .get(&group)
            .and_then(|(_, op)| self.history.get(op).map(|h| (*op, h)))
        else {
            return Ok(None);
        };
        Ok(Some(GroupCreationStatus {
            operation,
            index: h.index,
            intent: GroupCreationIntent::decode(&h.bytes)?,
        }))
    }
}
