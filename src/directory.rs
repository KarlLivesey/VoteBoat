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
//! Replicated directory publication and opt-in creation reservations.
//!
//! Initial assignments are an explicit trusted bootstrap plan. Publish commands
//! cannot transfer ownership, fence a source or activate a target. Host-controlled
//! authorization remains required before proposal. Durable progress comes solely
//! from the existing Raft/WAL/application/checkpoint contracts. Schema2 creation
//! reserves fresh identities but cannot bootstrap groups or activate ownership.
mod child_slots;
mod reparent_guards;
mod reparenting;
use crate::reparent_guard::*;
use crate::reparenting::*;
pub mod creation;
use crate::child_slots::*;
mod deletion;
mod namespace;
use crate::delegation::*;
use crate::deletion::*;
use crate::namespace_creation::*;
use crate::routing::codec::*;
use crate::transfer::{InsertionChild, TransferIntent, TransferIntentStatus};
use crate::transfer_publication::*;
use crate::{application::*, identity::*, log::*, routing::*};
pub use creation::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    mem::size_of,
};

pub const DIRECTORY_APPLICATION_SCHEMA: u64 = 1;
pub const CREATION_DIRECTORY_APPLICATION_SCHEMA: u64 = 2;
pub const NAMESPACE_DIRECTORY_APPLICATION_SCHEMA: u64 = 3;
pub const NAMESPACE_TRANSFER_DIRECTORY_APPLICATION_SCHEMA: u64 = 4;
pub const INSERTION_DIRECTORY_APPLICATION_SCHEMA: u64 = 5;
pub const RECURSIVE_INSERTION_DIRECTORY_APPLICATION_SCHEMA: u64 = 6;
pub const RETAINED_INSERTION_DIRECTORY_APPLICATION_SCHEMA: u64 = 8;
pub const CROSS_AUTHORITY_INSERTION_DIRECTORY_APPLICATION_SCHEMA: u64 = 7;
pub const MAX_DIRECTORY_MANIFESTS: usize = 256;
pub const MAX_DIRECTORY_OPERATIONS: usize = 4096;
pub const MAX_DIRECTORY_HISTORY_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_DIRECTORY_PUBLICATION_BYTES: usize = 32768;
pub const MAX_DIRECTORY_COMMAND_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_DIRECTORY_PENDING: usize = 8192;
pub const MAX_DIRECTORY_CONTROL_BYTES: usize = MAX_TRANSFER_PUBLICATION_BYTES + 128;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DirectoryLimits {
    pub operations: usize,
    pub history_bytes: usize,
}
impl DirectoryLimits {
    fn validate(self) -> Result<(), ApplicationError> {
        if self.operations == 0
            || self.operations > MAX_DIRECTORY_OPERATIONS
            || self.history_bytes == 0
            || self.history_bytes > MAX_DIRECTORY_HISTORY_BYTES
        {
            return Err(ApplicationError::DedupCapacity);
        }
        Ok(())
    }
}
/// Explicit trusted grants for already established groups. This is analogous to
/// a fixed group bootstrap, NOT permission to replace a live/unreachable owner.
/// External parent grants must be authenticated and verified by the caller.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectoryPlan {
    authority: GroupIdentity,
    manifests: BTreeMap<ResponsibilityIdentity, ResponsibilityManifest>,
}
impl DirectoryPlan {
    pub fn new(
        authority: GroupIdentity,
        manifests: Vec<ResponsibilityManifest>,
    ) -> Result<Self, (RoutingError, Vec<ResponsibilityManifest>)> {
        if let Err(error) = Self::validate(authority, &manifests) {
            return Err((error, manifests));
        }
        Ok(Self {
            authority,
            manifests: manifests
                .into_iter()
                .map(|m| (m.input().responsibility, m))
                .collect(),
        })
    }
    fn validate(
        authority: GroupIdentity,
        manifests: &Vec<ResponsibilityManifest>,
    ) -> Result<(), RoutingError> {
        if manifests.is_empty()
            || manifests.len() > MAX_DIRECTORY_MANIFESTS
            || manifests.capacity() > MAX_DIRECTORY_MANIFESTS
        {
            return Err(RoutingError::Capacity);
        }
        let mut children = BTreeSet::new();
        for (i, manifest) in manifests.iter().enumerate() {
            let input = manifest.input();
            if input.authority != authority {
                return Err(RoutingError::WrongOwner);
            }
            if input.generation.get() != 1 || input.state != ResponsibilityState::Active {
                return Err(RoutingError::EpochMismatch);
            }
            if manifests[..i]
                .iter()
                .any(|m| m.input().responsibility.id == input.responsibility.id)
            {
                return Err(RoutingError::DuplicateChild);
            }
            if let Some(parent) = input.parent {
                if parent.group != authority
                    && manifests
                        .iter()
                        .any(|m| m.input().responsibility == parent.responsibility)
                {
                    return Err(RoutingError::WrongParent);
                }
                if parent.group == authority {
                    let parent_manifest = manifests
                        .iter()
                        .find(|m| m.input().responsibility == parent.responsibility)
                        .ok_or(RoutingError::WrongParent)?;
                    if !delegates(parent_manifest, manifest) {
                        return Err(RoutingError::WrongChild);
                    }
                }
            }
            if let ExecutionMode::Delegated(entries) = &input.execution {
                for entry in entries {
                    if let RouteTarget::Child(child) = entry.target {
                        if !children.insert(child.responsibility) {
                            return Err(RoutingError::DuplicateChild);
                        }
                        if child.group != authority
                            && manifests
                                .iter()
                                .any(|m| m.input().responsibility == child.responsibility)
                        {
                            return Err(RoutingError::WrongChild);
                        }
                        if child.group == authority {
                            let child_manifest = manifests
                                .iter()
                                .find(|m| m.input().responsibility == child.responsibility)
                                .ok_or(RoutingError::WrongChild)?;
                            if !delegates(manifest, child_manifest) {
                                return Err(RoutingError::WrongChild);
                            }
                        }
                    }
                }
            }
            let mut current = Some(input.responsibility);
            let mut seen = [None; MAX_ROUTE_HOPS];
            for hop in 0..=MAX_ROUTE_HOPS {
                let Some(id) = current else { break };
                if hop == MAX_ROUTE_HOPS {
                    return Err(RoutingError::HopLimit);
                }
                if seen[..hop].contains(&Some(id)) {
                    return Err(RoutingError::Cycle);
                }
                seen[hop] = Some(id);
                current = manifests
                    .iter()
                    .find(|m| m.input().responsibility == id)
                    .and_then(|m| m.input().parent)
                    .filter(|p| p.group == authority)
                    .map(|p| p.responsibility);
            }
        }
        Ok(())
    }
    pub fn authority(&self) -> GroupIdentity {
        self.authority
    }
    pub fn manifests(&self) -> impl Iterator<Item = &ResponsibilityManifest> {
        self.manifests.values()
    }
    fn encoded_len(&self) -> usize {
        self.manifests.values().map(|m| 4 + manifest_len(m)).sum()
    }
}
fn delegates(parent: &ResponsibilityManifest, child: &ResponsibilityManifest) -> bool {
    let p = parent.input();
    let c = child.input();
    let ExecutionMode::Delegated(entries) = &p.execution else {
        return false;
    };
    c.parent
        == Some(ParentAuthority {
            responsibility: p.responsibility,
            group: p.authority,
        })
        && c.scheme == p.scheme
        && entries.iter().any(|entry| {
            entry.scope == c.scope
                && entry.target
                    == RouteTarget::Child(ChildAuthority {
                        responsibility: c.responsibility,
                        group: c.authority,
                        epoch: c.epoch,
                    })
        })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectoryCommand {
    pub expected: Option<RouteGeneration>,
    pub manifest: ResponsibilityManifest,
}
impl DirectoryCommand {
    pub fn encode(&self, max_bytes: usize) -> Result<Vec<u8>, ApplicationError> {
        let len = 20 + manifest_len(&self.manifest);
        if len > max_bytes || len > MAX_DIRECTORY_PUBLICATION_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut bytes = Vec::with_capacity(len);
        bytes.extend(b"VBDCMD01");
        bytes.extend(self.expected.map_or(0, RouteGeneration::get).to_le_bytes());
        bytes.extend((manifest_len(&self.manifest) as u32).to_le_bytes());
        put_manifest(&mut bytes, &self.manifest);
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, ApplicationError> {
        if bytes.len() > MAX_DIRECTORY_PUBLICATION_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut reader = Reader::new(bytes);
        if reader.take(8)? != b"VBDCMD01" {
            return Err(ApplicationError::InvalidCommand);
        }
        let expected = RouteGeneration::new(reader.u64()?);
        let len = reader.u32()? as usize;
        let manifest = read_manifest(reader.take(len)?)?;
        if !reader.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(Self { expected, manifest })
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DirectoryOutcome {
    Initialized,
    CreationReserved,
    CreationConflict,
    NamespacePublished(RouteGeneration),
    Published(RouteGeneration),
    GenerationMismatch,
    UnknownResponsibility,
    OwnershipChange,
    OperationConflict,
    TransferIntentRecorded,
    LifecycleBusy,
    TransferGroupBusy,
    TransferControlBusy,
    TransferEvidenceMismatch,
    TransferPublished(RouteGeneration),
    DelegationReserved,
    DelegationPublished(RouteGeneration),
    DelegationDeclined,
    DelegationCancelled,
    DeletionIntentRecorded,
    Deleted(RouteGeneration),
    ChildSlotRetired(RouteGeneration),
    ReparentGuarded,
    ReparentCancelled,
    Reparented,
    ReparentRejected(RoutingError),
}
#[allow(clippy::large_enum_variant)] // Bounded cold parsing; no extra heap indirection.
enum Request {
    Bootstrap,
    RetireChildSlot(RetireChildSlot),
    Reparent(ReparentPlan),
    ReparentGuard(PrepareReparent),
    CancelReparent(CancelReparent),
    ReleaseReparent(ReleaseReparentGuard),
    Deletion(DeletionIntent),
    DeletionCompletion(DeletionCompletion),
    Create(GroupCreationIntent),
    Namespace(NamespacePublication),
    Publish(DirectoryCommand),
    Transfer(TransferIntent),
    Publication(TransferPublication),
    Delegation(DelegationPlan),
    DelegationCompletion(DelegationCompletion),
    DelegationDecline(DelegationDecline),
    DelegationCancellation(DelegationCancellation),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DirectoryReceipt {
    pub index: u64,
    pub operation: OperationId,
    pub outcome: DirectoryOutcome,
    pub duplicate: bool,
}
impl ApplicationReceipt for DirectoryReceipt {
    fn index(&self) -> u64 {
        self.index
    }
    fn operation(&self) -> OperationId {
        self.operation
    }
    fn nested_bytes(&self, _: usize) -> Result<usize, ApplicationError> {
        Ok(0)
    }
}
#[derive(Clone)]
struct History {
    index: u64,
    bytes: Vec<u8>,
    outcome: DirectoryOutcome,
}
#[derive(Clone)]
pub struct Directory {
    plan: DirectoryPlan,
    limits: DirectoryLimits,
    applied: u64,
    initialized: bool,
    group_creation: bool,
    namespace_creation: bool,
    namespace_transfers: bool,
    responsibility_insertion: bool,
    recursive_insertion: bool,
    cross_authority_insertion: bool,
    retained_insertion: bool,
    namespace_deletion: bool,
    child_slot_retirement: bool,
    local_reparenting: bool,
    reparent_guards: bool,
    guarded_manifests: BTreeMap<ResponsibilityIdentity, OperationId>,
    guarded_operations: BTreeSet<OperationId>,
    reparent_cancellations: BTreeMap<OperationId, OperationId>,
    reparent_controls: BTreeSet<OperationId>,
    retired_slot_children: BTreeMap<ResponsibilityIdentity, GroupIdentity>,
    deletions: BTreeMap<ResponsibilityIdentity, OperationId>,
    deletion_publications: BTreeMap<OperationId, OperationId>,
    insertion_creations: BTreeSet<OperationId>,
    namespace_publications: BTreeMap<OperationId, OperationId>,
    manifests: BTreeMap<ResponsibilityIdentity, ResponsibilityManifest>,
    history: BTreeMap<OperationId, History>,
    history_bytes: usize,
    creations: BTreeMap<GroupIdentity, (ResponsibilityIdentity, OperationId)>,
    control_bytes: usize,
    publications: BTreeMap<OperationId, OperationId>,
    /// Locks reference existing bounded history rather than duplicate manifests.
    transfers: BTreeMap<ResponsibilityIdentity, OperationId>,
    transfer_targets: BTreeSet<GroupIdentity>,
    delegations: BTreeMap<ResponsibilityIdentity, OperationId>,
    delegation_publications: BTreeMap<OperationId, OperationId>,
    delegation_declines: BTreeMap<OperationId, OperationId>,
    delegation_cancellations: BTreeMap<OperationId, OperationId>,
}
impl Directory {
    pub fn new(
        plan: DirectoryPlan,
        limits: DirectoryLimits,
    ) -> Result<Self, (ApplicationError, DirectoryPlan)> {
        if let Err(error) = limits.validate() {
            return Err((error, plan));
        }
        let bootstrap_bytes = 46 + plan.encoded_len();
        if bootstrap_bytes > limits.history_bytes || bootstrap_bytes > MAX_DIRECTORY_COMMAND_BYTES {
            return Err((ApplicationError::DedupCapacity, plan));
        }
        Ok(Self {
            plan,
            limits,
            applied: 0,
            initialized: false,
            group_creation: false,
            namespace_creation: false,
            namespace_transfers: false,
            responsibility_insertion: false,
            recursive_insertion: false,
            cross_authority_insertion: false,
            retained_insertion: false,
            namespace_deletion: false,
            child_slot_retirement: false,
            local_reparenting: false,
            reparent_guards: false,
            guarded_manifests: BTreeMap::new(),
            guarded_operations: BTreeSet::new(),
            reparent_cancellations: BTreeMap::new(),
            reparent_controls: BTreeSet::new(),
            retired_slot_children: BTreeMap::new(),
            deletions: BTreeMap::new(),
            deletion_publications: BTreeMap::new(),
            insertion_creations: BTreeSet::new(),
            namespace_publications: BTreeMap::new(),
            manifests: BTreeMap::new(),
            history: BTreeMap::new(),
            history_bytes: 0,
            creations: BTreeMap::new(),
            control_bytes: 0,
            publications: BTreeMap::new(),
            transfers: BTreeMap::new(),
            transfer_targets: BTreeSet::new(),
            delegations: BTreeMap::new(),
            delegation_publications: BTreeMap::new(),
            delegation_declines: BTreeMap::new(),
            delegation_cancellations: BTreeMap::new(),
        })
    }
    /// Select before bootstrap. Schema2 history cannot be opened as legacy1.
    #[allow(clippy::result_large_err)]
    pub fn with_group_creation(mut self) -> Result<Self, (ApplicationError, Self)> {
        if self.applied != 0 || self.initialized || !self.history.is_empty() {
            return Err((ApplicationError::InvalidCommand, self));
        }
        self.group_creation = true;
        Ok(self)
    }
    /// Select schema3 before bootstrap; no live upgrade from schemas1/2.
    #[allow(clippy::result_large_err)]
    pub fn with_namespace_creation(self) -> Result<Self, (ApplicationError, Self)> {
        let mut next = self.with_group_creation()?;
        next.namespace_creation = true;
        Ok(next)
    }
    /// Select schema4 before bootstrap, allowing published fresh namespaces to
    /// enter the existing transfer protocol. No live upgrade from schemas1–3:
    /// their historical transfer refusals must retain the same replay outcome.
    #[allow(clippy::result_large_err)]
    pub fn with_namespace_transfers(self) -> Result<Self, (ApplicationError, Self)> {
        let mut next = self.with_namespace_creation()?;
        next.namespace_transfers = true;
        Ok(next)
    }
    /// Select schema5 before bootstrap; insertion publishes parent and children together.
    #[allow(clippy::result_large_err)]
    pub fn with_responsibility_insertion(self) -> Result<Self, (ApplicationError, Self)> {
        let mut next = self.with_namespace_transfers()?;
        next.responsibility_insertion = true;
        Ok(next)
    }
    /// Select schema6 before bootstrap; authorize nested insertion and dynamic parents.
    #[allow(clippy::result_large_err)]
    pub fn with_recursive_insertion(self) -> Result<Self, (ApplicationError, Self)> {
        let mut next = self.with_responsibility_insertion()?;
        next.recursive_insertion = true;
        Ok(next)
    }
    /// Select schema7 before bootstrap for insertion beneath a foreign parent.
    /// Foreign committed observations require authenticated host provenance.
    #[allow(clippy::result_large_err)]
    pub fn with_cross_authority_insertion(self) -> Result<Self, (ApplicationError, Self)> {
        let mut next = self.with_recursive_insertion()?;
        next.cross_authority_insertion = true;
        Ok(next)
    }
    /// Select schema8 before bootstrap for scoped child insertion with retained service.
    #[allow(clippy::result_large_err)]
    pub fn with_retained_insertion(self) -> Result<Self, (ApplicationError, Self)> {
        let mut next = self.with_cross_authority_insertion()?;
        next.retained_insertion = true;
        Ok(next)
    }

    /// Select schema9 before bootstrap. Tombstones and their source/child facts
    /// remain in the original bounded log/checkpoint; there is no live upgrade.
    #[allow(clippy::result_large_err)]
    pub fn with_namespace_deletion(self) -> Result<Self, (ApplicationError, Self)> {
        let mut next = self.with_retained_insertion()?;
        next.namespace_deletion = true;
        Ok(next)
    }
    /// Select schema10 before bootstrap for checked deleted-child slot retirement.
    #[allow(clippy::result_large_err)]
    pub fn with_child_slot_retirement(self) -> Result<Self, (ApplicationError, Self)> {
        let mut next = self.with_namespace_deletion()?;
        next.child_slot_retirement = true;
        Ok(next)
    }
    /// Select schema11 before bootstrap for atomic reparenting under one authority.
    #[allow(clippy::result_large_err)]
    pub fn with_local_reparenting(self) -> Result<Self, (ApplicationError, Self)> {
        let mut next = self.with_child_slot_retirement()?;
        next.local_reparenting = true;
        Ok(next)
    }
    /// Select schema12 before bootstrap for cross-authority prepare/cancel guards.
    #[allow(clippy::result_large_err)]
    pub fn with_reparent_guards(self) -> Result<Self, (ApplicationError, Self)> {
        let mut next = self.with_local_reparenting()?;
        next.reparent_guards = true;
        Ok(next)
    }
    fn control_command_limit(&self) -> usize {
        if self.namespace_deletion {
            MAX_DELETION_COMPLETION_BYTES.max(MAX_DIRECTORY_CONTROL_BYTES)
        } else {
            MAX_DIRECTORY_CONTROL_BYTES
        }
    }
    /// Local applied diagnostic, not a distributed linearizable directory read.
    pub fn manifest(&self, id: ResponsibilityIdentity) -> Option<&ResponsibilityManifest> {
        self.manifests.get(&id)
    }
    pub fn plan(&self) -> &DirectoryPlan {
        &self.plan
    }
    pub fn limits(&self) -> DirectoryLimits {
        self.limits
    }
    pub fn remaining_operations(&self) -> usize {
        self.limits.operations
            - (self.history.len()
                - self.publications.len()
                - self.delegation_publications.len()
                - self.delegation_cancellations.len()
                - self.namespace_publications.len()
                - self.deletion_publications.len()
                - self.reparent_controls.len())
    }
    /// Extra bounded control pool; successful publications never use ordinary history.
    pub fn control_history_capacity(&self) -> usize {
        self.limits.operations.min(MAX_DIRECTORY_MANIFESTS) * self.control_command_limit()
    }
    pub fn reserved_publication_bytes(&self) -> usize {
        self.guarded_operations.len() * 2 * MAX_REPARENT_COMPLETION_BYTES
            + (self.deletions.len() - self.deletion_publications.len())
                * MAX_DELETION_COMPLETION_BYTES
            + self.transfers.len() * MAX_TRANSFER_PUBLICATION_BYTES
            + self.delegations.len() * MAX_DIRECTORY_CONTROL_BYTES
            + if self.namespace_creation {
                (self.creations.len()
                    - self.namespace_publications.len()
                    - self.insertion_creations.len())
                    * MAX_NAMESPACE_PUBLICATION_BYTES
            } else {
                0
            }
    }
    /// Commit this exact plan/capacity binding before any publication. It uses
    /// an ordinary operation ID and retains its original request/result. Large
    /// plans require explicitly compatible log/transport command envelopes.
    pub fn bootstrap_command(&self, max_bytes: usize) -> Result<Vec<u8>, ApplicationError> {
        if !self.child_slot_retirement
            && self
                .plan
                .manifests
                .values()
                .any(ResponsibilityManifest::has_vacancies)
        {
            return Err(ApplicationError::UnsupportedSchema);
        }
        let len = 46 + self.plan.encoded_len();
        if len > max_bytes || len > MAX_DIRECTORY_COMMAND_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut bytes = Vec::with_capacity(len);
        bytes.extend(if self.reparent_guards {
            b"VBDINI12"
        } else if self.local_reparenting {
            b"VBDINI11"
        } else if self.child_slot_retirement {
            b"VBDINI10"
        } else if self.namespace_deletion {
            b"VBDINIT9"
        } else if self.retained_insertion {
            b"VBDINIT8"
        } else if self.cross_authority_insertion {
            b"VBDINIT7"
        } else if self.recursive_insertion {
            b"VBDINIT6"
        } else if self.responsibility_insertion {
            b"VBDINIT5"
        } else if self.namespace_transfers {
            b"VBDINIT4"
        } else if self.namespace_creation {
            b"VBDINIT3"
        } else if self.group_creation {
            b"VBDINIT2"
        } else {
            b"VBDINIT1"
        });
        put_group(&mut bytes, self.plan.authority);
        bytes.extend((self.limits.operations as u32).to_le_bytes());
        bytes.extend((self.limits.history_bytes as u64).to_le_bytes());
        bytes.extend((self.plan.manifests.len() as u16).to_le_bytes());
        for manifest in self.plan.manifests.values() {
            bytes.extend((manifest_len(manifest) as u32).to_le_bytes());
            put_manifest(&mut bytes, manifest);
        }
        Ok(bytes)
    }
    pub fn is_initialized(&self) -> bool {
        self.initialized
    }
    fn request(&self, bytes: &[u8]) -> Result<Request, ApplicationError> {
        if bytes.len() > MAX_DIRECTORY_COMMAND_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let request = if bytes.starts_with(b"VBDINIT1")
            || bytes.starts_with(b"VBDINIT2")
            || bytes.starts_with(b"VBDINIT3")
            || bytes.starts_with(b"VBDINIT4")
            || bytes.starts_with(b"VBDINIT5")
            || bytes.starts_with(b"VBDINIT6")
            || bytes.starts_with(b"VBDINIT7")
            || bytes.starts_with(b"VBDINIT8")
            || bytes.starts_with(b"VBDINIT9")
            || bytes.starts_with(b"VBDINI10")
            || bytes.starts_with(b"VBDINI11")
            || bytes.starts_with(b"VBDINI12")
        {
            if bytes != self.bootstrap_command(bytes.len())? {
                return Err(ApplicationError::InvalidCommand);
            }
            Ok(Request::Bootstrap)
        } else {
            if !self.initialized {
                return Err(ApplicationError::NotApplied);
            }
            if self.reparent_guards && bytes.starts_with(b"VBXRGR01") {
                PrepareReparent::decode(bytes).map(Request::ReparentGuard)
            } else if self.reparent_guards && bytes.starts_with(b"VBXRCN01") {
                CancelReparent::decode(bytes).map(Request::CancelReparent)
            } else if self.reparent_guards && bytes.starts_with(b"VBXRAB01") {
                ReleaseReparentGuard::decode(bytes).map(Request::ReleaseReparent)
            } else if self.local_reparenting && bytes.starts_with(b"VBRPAR01") {
                ReparentPlan::decode(bytes).map(Request::Reparent)
            } else if self.child_slot_retirement && bytes.starts_with(b"VBRSLOT1") {
                RetireChildSlot::decode(bytes).map(Request::RetireChildSlot)
            } else if self.namespace_deletion && bytes.starts_with(b"VBDDEL01") {
                DeletionIntent::decode(bytes).map(Request::Deletion)
            } else if self.namespace_deletion && bytes.starts_with(b"VBDDCM01") {
                DeletionCompletion::decode(bytes).map(Request::DeletionCompletion)
            } else if bytes.starts_with(b"VBNSPUB1") {
                if !self.namespace_creation {
                    return Err(ApplicationError::InvalidCommand);
                }
                NamespacePublication::decode(bytes).map(Request::Namespace)
            } else if bytes.starts_with(b"VBGCRT01") {
                if !self.group_creation {
                    return Err(ApplicationError::InvalidCommand);
                }
                GroupCreationIntent::decode(bytes).map(Request::Create)
            } else if bytes.starts_with(b"VBTINT01")
                || bytes.starts_with(b"VBTINT02")
                || (self.responsibility_insertion && bytes.starts_with(b"VBTINT03"))
                || (self.recursive_insertion && bytes.starts_with(b"VBTINT04"))
                || (self.cross_authority_insertion && bytes.starts_with(b"VBTINT05"))
                || (self.retained_insertion && bytes.starts_with(b"VBTINT06"))
            {
                TransferIntent::decode(bytes).map(Request::Transfer)
            } else if bytes.starts_with(b"VBTPUB01")
                || (self.retained_insertion && bytes.starts_with(b"VBTPUB02"))
            {
                TransferPublication::decode(bytes).map(Request::Publication)
            } else if bytes.starts_with(b"VBDPLAN1")
                || (self.recursive_insertion && bytes.starts_with(b"VBDPLAN2"))
                || (self.cross_authority_insertion && bytes.starts_with(b"VBDPLAN3"))
                || (self.retained_insertion && bytes.starts_with(b"VBDPLAN4"))
            {
                DelegationPlan::decode(bytes).map(Request::Delegation)
            } else if bytes.starts_with(b"VBDCOMP1") {
                DelegationCompletion::decode(bytes).map(Request::DelegationCompletion)
            } else if bytes.starts_with(b"VBDDECL1") {
                DelegationDecline::decode(bytes).map(Request::DelegationDecline)
            } else if bytes.starts_with(b"VBDCANC1") {
                DelegationCancellation::decode(bytes).map(Request::DelegationCancellation)
            } else {
                DirectoryCommand::decode(bytes).map(Request::Publish)
            }
        }?;
        let intent = match &request {
            Request::Transfer(i) => Some(i),
            Request::Publication(p) => Some(p.intent()),
            Request::DelegationCompletion(c) => Some(c.decision.publication.intent()),
            Request::DelegationDecline(d) => Some(d.intent()),
            Request::DelegationCancellation(c) => Some(c.decline.decline.intent()),
            _ => None,
        };
        if !self.retained_insertion && intent.is_some_and(TransferIntent::is_retained_insertion) {
            return Err(ApplicationError::InvalidCommand);
        }
        if !self.cross_authority_insertion
            && intent.is_some_and(TransferIntent::cross_authority_insertion)
        {
            return Err(ApplicationError::InvalidCommand);
        }
        if !self.child_slot_retirement {
            let vacant_intent =
                |i: &TransferIntent| i.before().has_vacancies() || i.after().has_vacancies();
            let vacancies = match &request {
                Request::Publish(c) => c.manifest.has_vacancies(),
                Request::Namespace(c) => c.manifest.has_vacancies(),
                Request::Deletion(c) => c.before.has_vacancies(),
                Request::DeletionCompletion(c) => c.intent.intent.before.has_vacancies(),
                Request::Delegation(c) => {
                    c.parent().has_vacancies()
                        || c.before().has_vacancies()
                        || c.after().has_vacancies()
                }
                _ => intent.is_some_and(vacant_intent),
            };
            if vacancies {
                return Err(ApplicationError::UnsupportedSchema);
            }
        }
        Ok(request)
    }
    pub fn readiness_requirements(&self) -> crate::raft::ReadinessRequirements {
        crate::raft::ReadinessRequirements {
            application_schema: self.schema_version(),
            command_bytes: self
                .control_command_limit()
                .max(46 + self.plan.encoded_len())
                .max(if self.reparent_guards {
                    MAX_REPARENT_PREPARE_BYTES
                } else {
                    0
                })
                .max(if self.group_creation {
                    MAX_GROUP_CREATION_BYTES
                } else {
                    0
                }),
            snapshot_bytes: 58
                + self.plan.encoded_len()
                + if self.reparent_guards { 112 } else { 56 } * self.limits.operations
                + self.limits.history_bytes
                + self.control_history_capacity(),
        }
    }
    fn publish(&mut self, command: DirectoryCommand) -> DirectoryOutcome {
        let input = command.manifest.input();
        let id = input.responsibility;
        if self.transfers.contains_key(&id)
            || self.delegations.contains_key(&id)
            || self.deletion_busy(id)
        {
            return DirectoryOutcome::LifecycleBusy;
        }
        let Some(grant) = self
            .plan
            .manifests
            .get(&id)
            .or_else(|| self.manifests.get(&id))
        else {
            return DirectoryOutcome::UnknownResponsibility;
        };
        if let Some(prior) = self.manifests.get(&id) {
            let old = prior.input();
            if command.expected != Some(old.generation)
                || old.generation.get().checked_add(1) != Some(input.generation.get())
            {
                return DirectoryOutcome::GenerationMismatch;
            }
            // Metadata publication cannot create a transfer, reparenting, adapter
            // migration, revival or changed authoritative ownership by itself.
            if input.parent != old.parent
                || input.authority != old.authority
                || input.application != old.application
                || input.scheme != old.scheme
                || input.scope != old.scope
                || input.epoch != old.epoch
                || input.state != old.state
                || input.execution != old.execution
            {
                return DirectoryOutcome::OwnershipChange;
            }
        } else {
            if command.expected.is_some() {
                return DirectoryOutcome::GenerationMismatch;
            }
            if &command.manifest != grant {
                return DirectoryOutcome::OwnershipChange;
            }
            // Local child publication waits for its parent to be committed/applied.
            if let Some(parent) = input.parent.filter(|p| p.group == self.plan.authority) {
                if !self
                    .manifests
                    .get(&parent.responsibility)
                    .is_some_and(|p| delegates(p, &command.manifest))
                {
                    return DirectoryOutcome::GenerationMismatch;
                }
            }
        }
        let generation = input.generation;
        self.manifests.insert(id, command.manifest);
        DirectoryOutcome::Published(generation)
    }
    fn insertion_permitted(
        &self,
        before: &ResponsibilityManifest,
        children: &[InsertionChild],
    ) -> bool {
        if !self.responsibility_insertion {
            return false;
        }
        let b = before.input();
        if self.recursive_insertion {
            let mut current = b.responsibility;
            let mut seen = [None; MAX_ROUTE_HOPS];
            let mut rooted = false;
            for hop in 0..MAX_ROUTE_HOPS - 1 {
                if seen[..hop].contains(&Some(current)) {
                    return false;
                }
                seen[hop] = Some(current);
                let Some(m) = self.manifests.get(&current) else {
                    return false;
                };
                match m.input().parent {
                    None => {
                        rooted = true;
                        break;
                    }
                    Some(p) if p.group == self.plan.authority => current = p.responsibility,
                    Some(_) if self.cross_authority_insertion => {
                        rooted = true;
                        break;
                    }
                    _ => return false,
                }
            }
            if !rooted {
                return false;
            }
        }
        children.iter().all(|child| {
            let ExecutionMode::Single(group) = child.manifest.input().execution else {
                return false;
            };
            !self
                .manifests
                .keys()
                .any(|id| id.id == child.manifest.input().responsibility.id)
                && !self.insertion_creations.contains(&child.creation)
                && self
                    .group_creation_at(self.applied, group)
                    .ok()
                    .flatten()
                    .is_some_and(|status| {
                        status.operation == child.creation
                            && status.index == child.creation_index
                            && status.intent.expected == b.generation
                            && InsertionChild::from_creation(child.manifest.clone(), &status)
                                .ok()
                                .as_ref()
                                == Some(child)
                    })
        })
    }
    fn begin_transfer(
        &mut self,
        operation: OperationId,
        intent: TransferIntent,
    ) -> DirectoryOutcome {
        if self.delegation_declines.contains_key(&operation) {
            return DirectoryOutcome::DelegationDeclined;
        }
        let before = intent.before().input();
        if before.authority != self.plan.authority
            || (!self.plan.manifests.contains_key(&before.responsibility)
                && !(self.namespace_transfers
                    && self.manifests.contains_key(&before.responsibility)))
        {
            return DirectoryOutcome::UnknownResponsibility;
        }
        if self.deletion_busy(before.responsibility)
            || before
                .parent
                .filter(|p| p.group == self.plan.authority)
                .is_some_and(|p| self.deletion_busy(p.responsibility))
            || self.transfers.contains_key(&before.responsibility)
            || self.delegations.contains_key(&before.responsibility)
        {
            return DirectoryOutcome::LifecycleBusy;
        }
        if self.manifests.get(&before.responsibility) != Some(intent.before()) {
            return DirectoryOutcome::GenerationMismatch;
        }
        if !intent.permits_operation(operation) {
            return DirectoryOutcome::TransferEvidenceMismatch;
        }
        if let Some(parent) = before.parent.filter(|p| p.group == self.plan.authority) {
            let Some(binding) = intent.delegation() else {
                return DirectoryOutcome::TransferEvidenceMismatch;
            };
            if self.delegations.get(&parent.responsibility) != Some(&binding.operation)
                || self
                    .delegation_reservation_at(self.applied, binding.operation)
                    .ok()
                    .flatten()
                    .and_then(|s| s.child_intent(binding.configuration).ok())
                    .as_ref()
                    != Some(&intent)
            {
                return DirectoryOutcome::TransferEvidenceMismatch;
            }
        }
        if intent.insertion_children().is_some()
            && before
                .parent
                .is_some_and(|p| p.group != self.plan.authority)
            && (!self.cross_authority_insertion || intent.delegation().is_none())
        {
            return DirectoryOutcome::TransferEvidenceMismatch;
        }
        if let Some(children) = intent.insertion_children() {
            if !self.insertion_permitted(intent.before(), children) {
                return DirectoryOutcome::TransferEvidenceMismatch;
            }
        }
        let targets = intent.targets();
        for target in &targets {
            let RouteTarget::Group(group) = target.target else {
                unreachable!("checked intent")
            };
            if self
                .retired_slot_children
                .values()
                .any(|g| g.id == group.id)
                || self.transfer_targets.contains(&group)
                || self
                    .plan
                    .manifests
                    .values()
                    .chain(self.manifests.values())
                    .any(|m| {
                        let input = m.input();
                        input.authority == group
                            || input.parent.is_some_and(|p| p.group == group)
                            || match &input.execution {
                                ExecutionMode::Single(g) => *g == group,
                                ExecutionMode::Partitioned(routes)
                                | ExecutionMode::Delegated(routes) => {
                                    routes.iter().any(|r| match r.target {
                                        RouteTarget::Vacant => false,
                                        RouteTarget::Group(g) => g == group,
                                        RouteTarget::Child(c) => c.group == group,
                                    })
                                }
                            }
                    })
            {
                return DirectoryOutcome::TransferGroupBusy;
            }
        }
        if self.control_bytes + self.reserved_publication_bytes() + MAX_TRANSFER_PUBLICATION_BYTES
            > self.control_history_capacity()
        {
            return DirectoryOutcome::TransferControlBusy;
        }
        for target in targets {
            let RouteTarget::Group(group) = target.target else {
                unreachable!("checked intent")
            };
            self.transfer_targets.insert(group);
        }
        self.transfers.insert(before.responsibility, operation);
        DirectoryOutcome::TransferIntentRecorded
    }
    /// Local diagnostic/read capability. Serving requires the same quorum-backed
    /// read barrier as manifest reads; LifecycleDirectory exposes that query.
    pub fn transfer_intent_at(
        &self,
        required: u64,
        operation: OperationId,
    ) -> Result<Option<TransferIntentStatus>, ApplicationError> {
        if required > self.applied {
            return Err(ApplicationError::NotApplied);
        }
        let Some(record) = self.history.get(&operation) else {
            return Ok(None);
        };
        if record.outcome != DirectoryOutcome::TransferIntentRecorded {
            return Ok(None);
        }
        Ok(Some(TransferIntentStatus {
            operation,
            index: record.index,
            intent: TransferIntent::decode(&record.bytes)?,
        }))
    }
    fn publication_permitted(&self, publication: &TransferPublication) -> bool {
        let id = publication.intent().before().input().responsibility;
        self.transfers.get(&id) == Some(&publication.operation())
            && self.manifests.get(&id) == Some(publication.intent().before())
            && self
                .transfer_intent_at(self.applied, publication.operation())
                .ok()
                .flatten()
                .is_some_and(|s| s.intent == *publication.intent())
    }
    fn complete_transfer(
        &mut self,
        operation: OperationId,
        publication: TransferPublication,
    ) -> DirectoryOutcome {
        if !self.publication_permitted(&publication) {
            return DirectoryOutcome::TransferEvidenceMismatch;
        }
        let id = publication.intent().before().input().responsibility;
        let generation = publication.intent().after().input().generation;
        self.manifests
            .insert(id, publication.intent().after().clone());
        if let Some(children) = publication.intent().insertion_children() {
            for child in children {
                self.manifests.insert(
                    child.manifest.input().responsibility,
                    child.manifest.clone(),
                );
                self.insertion_creations.insert(child.creation);
            }
        }
        self.transfers.remove(&id);
        self.publications.insert(publication.operation(), operation);
        DirectoryOutcome::TransferPublished(generation)
    }
    /// Local applied diagnostic. Use LifecycleDirectory with a quorum read barrier
    /// before treating this decision as foreign activation evidence.
    pub fn transfer_publication_at(
        &self,
        required: u64,
        operation: OperationId,
    ) -> Result<Option<TransferPublicationStatus>, ApplicationError> {
        if required > self.applied {
            return Err(ApplicationError::NotApplied);
        }
        let Some(record) = self
            .publications
            .get(&operation)
            .and_then(|op| self.history.get(op).map(|h| (*op, h)))
        else {
            return Ok(None);
        };
        Ok(Some(TransferPublicationStatus {
            publication_operation: record.0,
            index: record.1.index,
            publication: TransferPublication::decode(&record.1.bytes)?,
        }))
    }
    fn begin_delegation(
        &mut self,
        operation: OperationId,
        plan: DelegationPlan,
    ) -> DirectoryOutcome {
        let parent = plan.parent().input();
        if parent.authority != self.plan.authority
            || (!self.plan.manifests.contains_key(&parent.responsibility)
                && !(self.recursive_insertion
                    && self.manifests.contains_key(&parent.responsibility)))
        {
            return DirectoryOutcome::UnknownResponsibility;
        }
        if let Some(children) = plan.insertion_children() {
            if !self.recursive_insertion
                || children.iter().any(|c| c.creation == operation)
                || if plan.before().input().authority == self.plan.authority {
                    !self.insertion_permitted(plan.before(), children)
                } else {
                    !self.cross_authority_insertion
                        || children.iter().any(|c| {
                            self.manifests
                                .keys()
                                .any(|id| id.id == c.manifest.input().responsibility.id)
                        })
                }
            {
                return DirectoryOutcome::TransferEvidenceMismatch;
            }
        }
        if operation == plan.child_operation() {
            return DirectoryOutcome::TransferEvidenceMismatch;
        }
        if self.deletion_busy(parent.responsibility)
            || self.deletion_busy(plan.before().input().responsibility)
            || self.delegations.contains_key(&parent.responsibility)
            || self.transfers.contains_key(&parent.responsibility)
        {
            return DirectoryOutcome::LifecycleBusy;
        }
        if self.manifests.get(&parent.responsibility) != Some(plan.parent()) {
            return DirectoryOutcome::GenerationMismatch;
        }
        if plan.before().input().authority == self.plan.authority
            && self.manifests.get(&plan.before().input().responsibility) != Some(plan.before())
        {
            return DirectoryOutcome::GenerationMismatch;
        }
        if self.control_bytes + self.reserved_publication_bytes() + MAX_DIRECTORY_CONTROL_BYTES
            > self.control_history_capacity()
        {
            return DirectoryOutcome::TransferControlBusy;
        }
        self.delegations.insert(parent.responsibility, operation);
        DirectoryOutcome::DelegationReserved
    }
    pub fn delegation_reservation_at(
        &self,
        required: u64,
        operation: OperationId,
    ) -> Result<Option<DelegationReservationStatus>, ApplicationError> {
        if required > self.applied {
            return Err(ApplicationError::NotApplied);
        }
        let Some(h) = self
            .history
            .get(&operation)
            .filter(|h| h.outcome == DirectoryOutcome::DelegationReserved)
        else {
            return Ok(None);
        };
        Ok(Some(DelegationReservationStatus {
            operation,
            index: h.index,
            plan: DelegationPlan::decode(&h.bytes)?,
        }))
    }
    fn decline_delegation(
        &mut self,
        operation: OperationId,
        decline: DelegationDecline,
    ) -> DirectoryOutcome {
        let intent = decline.intent();
        let before = intent.before().input();
        let binding = intent.delegation().expect("checked decline");
        let Some(current) = self.manifests.get(&before.responsibility) else {
            return DirectoryOutcome::UnknownResponsibility;
        };
        if before.authority != self.plan.authority || current.input().parent != before.parent {
            return DirectoryOutcome::TransferEvidenceMismatch;
        }
        if operation == binding.child_operation
            || self
                .delegation_declines
                .contains_key(&binding.child_operation)
        {
            return DirectoryOutcome::TransferEvidenceMismatch;
        }
        // Successful original intents remain queryable after completion. Never
        // revoke one, even when the live transfer lock has already been removed.
        if self
            .transfer_intent_at(self.applied, binding.child_operation)
            .ok()
            .flatten()
            .is_some()
        {
            return DirectoryOutcome::LifecycleBusy;
        }
        if before
            .parent
            .is_some_and(|p| p.group == self.plan.authority)
            && self
                .delegation_reservation_at(self.applied, binding.operation)
                .ok()
                .flatten()
                .and_then(|s| s.child_intent(binding.configuration).ok())
                .as_ref()
                != Some(intent)
        {
            return DirectoryOutcome::TransferEvidenceMismatch;
        }
        self.delegation_declines
            .insert(binding.child_operation, operation);
        DirectoryOutcome::DelegationDeclined
    }
    pub fn delegation_decline_at(
        &self,
        required: u64,
        child_operation: OperationId,
    ) -> Result<Option<DelegationDeclineStatus>, ApplicationError> {
        if required > self.applied {
            return Err(ApplicationError::NotApplied);
        }
        let Some(operation) = self.delegation_declines.get(&child_operation) else {
            return Ok(None);
        };
        let h = self.history.get(operation).expect("retained decline");
        Ok(Some(DelegationDeclineStatus {
            operation: *operation,
            index: h.index,
            decline: DelegationDecline::decode(&h.bytes)?,
        }))
    }
    fn cancellation_permitted(&self, cancellation: &DelegationCancellation) -> bool {
        let Some(reservation) = self
            .delegation_reservation_at(self.applied, cancellation.reservation)
            .ok()
            .flatten()
        else {
            return false;
        };
        let parent = reservation.plan.parent().input();
        if self.delegations.get(&parent.responsibility) != Some(&reservation.operation)
            || self.manifests.get(&parent.responsibility) != Some(reservation.plan.parent())
            || reservation.index != cancellation.reservation_index
            || reservation
                .child_intent(cancellation.parent_configuration)
                .ok()
                .as_ref()
                != Some(cancellation.decline.decline.intent())
        {
            return false;
        }
        if reservation.plan.before().input().authority == self.plan.authority
            && (cancellation.child_configuration != cancellation.parent_configuration
                || self
                    .delegation_decline_at(self.applied, reservation.plan.child_operation())
                    .ok()
                    .flatten()
                    .as_ref()
                    != Some(&cancellation.decline))
        {
            return false;
        }
        true
    }
    fn cancel_delegation(
        &mut self,
        operation: OperationId,
        cancellation: DelegationCancellation,
    ) -> DirectoryOutcome {
        if !self.cancellation_permitted(&cancellation) {
            return DirectoryOutcome::TransferEvidenceMismatch;
        }
        let reservation = self
            .delegation_reservation_at(self.applied, cancellation.reservation)
            .expect("checked reservation")
            .expect("checked reservation");
        self.delegations
            .remove(&reservation.plan.parent().input().responsibility);
        self.delegation_cancellations
            .insert(reservation.operation, operation);
        DirectoryOutcome::DelegationCancelled
    }
    pub fn delegation_cancellation_at(
        &self,
        required: u64,
        reservation: OperationId,
    ) -> Result<Option<DelegationCancellationStatus>, ApplicationError> {
        if required > self.applied {
            return Err(ApplicationError::NotApplied);
        }
        let Some(operation) = self.delegation_cancellations.get(&reservation) else {
            return Ok(None);
        };
        let h = self.history.get(operation).expect("retained cancellation");
        Ok(Some(DelegationCancellationStatus {
            operation: *operation,
            index: h.index,
            cancellation: DelegationCancellation::decode(&h.bytes)?,
        }))
    }
    fn delegation_permitted(&self, completion: &DelegationCompletion) -> bool {
        let Some(status) = self
            .delegation_reservation_at(self.applied, completion.reservation)
            .ok()
            .flatten()
        else {
            return false;
        };
        let parent = status.plan.parent().input();
        if self.delegations.get(&parent.responsibility) != Some(&completion.reservation)
            || self.manifests.get(&parent.responsibility) != Some(status.plan.parent())
            || status.index != completion.reservation_index
            || status
                .child_intent(completion.parent_configuration)
                .ok()
                .as_ref()
                != Some(completion.decision.publication.intent())
            || status.plan.child_operation() != completion.decision.publication.operation()
        {
            return false;
        }
        // Local provenance is verifiable and cannot be replaced by host assertions.
        if status.plan.before().input().authority == self.plan.authority
            && (completion.child_configuration != completion.parent_configuration
                || self
                    .transfer_publication_at(self.applied, status.plan.child_operation())
                    .ok()
                    .flatten()
                    .as_ref()
                    != Some(&completion.decision))
        {
            return false;
        }
        true
    }
    fn complete_delegation(
        &mut self,
        operation: OperationId,
        completion: DelegationCompletion,
    ) -> DirectoryOutcome {
        if !self.delegation_permitted(&completion) {
            return DirectoryOutcome::TransferEvidenceMismatch;
        }
        let status = self
            .delegation_reservation_at(self.applied, completion.reservation)
            .expect("checked reservation")
            .expect("checked reservation");
        let updated = status.plan.updated_parent().expect("checked generation");
        let input = updated.input();
        let id = input.responsibility;
        let generation = input.generation;
        self.manifests.insert(id, updated);
        self.delegations.remove(&id);
        self.delegation_publications
            .insert(completion.reservation, operation);
        DirectoryOutcome::DelegationPublished(generation)
    }
    pub fn delegation_publication_at(
        &self,
        required: u64,
        reservation: OperationId,
    ) -> Result<Option<DelegationPublicationStatus>, ApplicationError> {
        if required > self.applied {
            return Err(ApplicationError::NotApplied);
        }
        let Some((operation, h)) = self
            .delegation_publications
            .get(&reservation)
            .and_then(|op| self.history.get(op).map(|h| (*op, h)))
        else {
            return Ok(None);
        };
        Ok(Some(DelegationPublicationStatus {
            operation,
            index: h.index,
            completion: DelegationCompletion::decode(&h.bytes)?,
        }))
    }
    fn execute(
        &mut self,
        index: u64,
        operation: OperationId,
        bytes: &[u8],
    ) -> Result<DirectoryReceipt, ApplicationError> {
        let command = self.request(bytes)?;
        let (outcome, duplicate) = if let Some(old) = self.history.get(&operation) {
            (
                if old.bytes == bytes {
                    old.outcome
                } else {
                    DirectoryOutcome::OperationConflict
                },
                true,
            )
        } else {
            let control = matches!(&command,Request::DeletionCompletion(c) if self.deletion_permitted(operation,c))
                || matches!(&command,Request::Publication(p) if self.publication_permitted(p))
                || matches!(&command,Request::Namespace(p) if self.namespace_permitted(p))
                || matches!(&command,Request::DelegationCompletion(c) if self.delegation_permitted(c))
                || matches!(&command,Request::DelegationCancellation(c) if self.cancellation_permitted(c))
                || matches!(&command,Request::CancelReparent(c) if self.cancel_reparent_permitted(operation,*c))
                || matches!(&command,Request::ReleaseReparent(c) if self.guarded_operations.contains(&c.decision.guard) && self.release_reparent_permitted(operation,*c));
            if !control
                && (self.remaining_operations() == 0
                    || self.history_bytes + bytes.len() > self.limits.history_bytes)
                || control
                    && (bytes.len() > self.control_command_limit()
                        || self.control_bytes + bytes.len() > self.control_history_capacity())
            {
                return Err(ApplicationError::DedupCapacity);
            }
            let guarded_before = (!self.guarded_manifests.is_empty()).then(|| self.clone());
            let mut outcome = match command {
                Request::ReparentGuard(p) => self.prepare_reparent(operation, p),
                Request::CancelReparent(c) => self.cancel_reparent(index, operation, c),
                Request::ReleaseReparent(c) => self.release_reparent(operation, c),
                Request::RetireChildSlot(r) => self.retire_child_slot(r),
                Request::Reparent(plan) => self.reparent(plan),
                Request::Deletion(i) => self.begin_deletion(operation, i),
                Request::DeletionCompletion(c) => self.complete_deletion(operation, c),
                Request::Publish(command) => self.publish(command),
                Request::Create(intent) => self.reserve_group_creation(operation, intent),
                Request::Namespace(p) => self.complete_namespace(operation, p),
                Request::Transfer(intent) => self.begin_transfer(operation, intent),
                Request::Publication(publication) => self.complete_transfer(operation, publication),
                Request::Delegation(plan) => self.begin_delegation(operation, plan),
                Request::DelegationCompletion(completion) => {
                    self.complete_delegation(operation, completion)
                }
                Request::DelegationDecline(decline) => self.decline_delegation(operation, decline),
                Request::DelegationCancellation(cancellation) => {
                    self.cancel_delegation(operation, cancellation)
                }
                Request::Bootstrap => {
                    self.initialized = true;
                    DirectoryOutcome::Initialized
                }
            };
            if let Some(before) = guarded_before {
                if !self.guarded_state_preserved(&before) {
                    *self = before;
                    outcome = DirectoryOutcome::LifecycleBusy;
                }
            }
            if control && matches!(outcome, DirectoryOutcome::ReparentCancelled) {
                self.reparent_controls.insert(operation);
            }
            self.history.insert(
                operation,
                History {
                    index,
                    bytes: bytes.to_vec(),
                    outcome,
                },
            );
            if control {
                self.control_bytes += bytes.len();
            } else {
                self.history_bytes += bytes.len();
            }
            (outcome, false)
        };
        Ok(DirectoryReceipt {
            index,
            operation,
            outcome,
            duplicate,
        })
    }
}
impl StateMachine for Directory {
    type Receipt = DirectoryReceipt;
    fn validate_group(&self, group: GroupIdentity) -> Result<(), ApplicationError> {
        if group != self.plan.authority {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(())
    }
    fn deployment_requirements(&self) -> Option<crate::raft::ReadinessRequirements> {
        Some(self.readiness_requirements())
    }
    fn applied_index(&self) -> u64 {
        self.applied
    }
    fn apply_batch(
        &mut self,
        entries: &[LogEntry],
    ) -> Result<Vec<Self::Receipt>, ApplicationError> {
        let mut next = self.clone();
        let mut receipts = Vec::with_capacity(
            entries
                .iter()
                .filter(|e| matches!(e.payload, EntryPayload::Command { .. }))
                .count(),
        );
        for entry in entries {
            if next.applied.checked_add(1) != Some(entry.index) {
                return Err(ApplicationError::IndexGap);
            }
            if let EntryPayload::Command { operation, bytes } = &entry.payload {
                receipts.push(next.execute(entry.index, *operation, bytes)?);
            }
            next.applied = entry.index;
        }
        *self = next;
        Ok(receipts)
    }
}
impl BoundedStateMachine for Directory {
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        entries
            .iter()
            .filter(|e| matches!(e.payload, EntryPayload::Command { .. }))
            .count()
            .checked_mul(size_of::<DirectoryReceipt>())
            .ok_or(ApplicationError::ReceiptBudget)
    }
}
impl ProposalAdmission for Directory {
    fn validate_proposal<'a>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError> {
        self.request(bytes)?;
        if self.reparent_guards {
            return self.admit_guarded(operation, bytes, pending);
        }
        // At most one successful decision per open lifecycle uses its reserve.
        // Other distinct requests still reserve ordinary history for losing outcomes.
        let mut reserved: BTreeMap<OperationId, (usize, Option<OperationId>)> = BTreeMap::new();
        let mut reserve = |id, bytes: &[u8]| -> Result<(), ApplicationError> {
            let request = self.request(bytes)?;
            if self.history.contains_key(&id) {
                return Ok(());
            }
            let lifecycle = match request {
                Request::DeletionCompletion(c) if self.deletion_permitted(id, &c) => {
                    Some(c.intent.operation)
                }
                Request::Namespace(p) if self.namespace_permitted(&p) => Some(p.creation),
                Request::Publication(p) if self.publication_permitted(&p) => Some(p.operation()),
                Request::DelegationCompletion(c) if self.delegation_permitted(&c) => {
                    Some(c.reservation)
                }
                Request::DelegationCancellation(c) if self.cancellation_permitted(&c) => {
                    Some(c.reservation)
                }
                _ => None,
            };
            reserved
                .entry(id)
                .and_modify(|(size, old)| {
                    *size = (*size).max(bytes.len());
                    if *old != lifecycle {
                        *old = None;
                    }
                })
                .or_insert((bytes.len(), lifecycle));
            if reserved.len() > 2 * self.limits.operations {
                return Err(ApplicationError::DedupCapacity);
            }
            Ok(())
        };
        for (position, (id, request)) in pending.enumerate() {
            if position >= MAX_DIRECTORY_PENDING {
                return Err(ApplicationError::DedupCapacity);
            }
            reserve(id, request)?;
        }
        reserve(operation, bytes)?;
        let mut credited = BTreeSet::new();
        let mut count = 0usize;
        let mut total = 0usize;
        for (size, lifecycle) in reserved.values() {
            if lifecycle.is_some_and(|op| credited.insert(op)) {
                continue;
            }
            count += 1;
            total = total
                .checked_add(*size)
                .ok_or(ApplicationError::DedupCapacity)?;
        }
        if count > self.remaining_operations()
            || total > self.limits.history_bytes - self.history_bytes
        {
            return Err(ApplicationError::DedupCapacity);
        }
        Ok(size_of::<DirectoryReceipt>())
    }
}
impl ReadableStateMachine for Directory {
    type Query = ResponsibilityIdentity;
    type ReadResult = Option<ResponsibilityManifest>;
    fn read_at(
        &self,
        required: u64,
        query: Self::Query,
    ) -> Result<Self::ReadResult, ApplicationError> {
        if required > self.applied {
            return Err(ApplicationError::NotApplied);
        }
        Ok(self.manifest(query).cloned())
    }
}
impl BoundedReadableStateMachine for Directory {
    fn query_bytes(&self, _: &Self::Query, _: usize) -> Result<usize, ApplicationError> {
        Ok(0)
    }
    fn read_result_bound(&self, query: &Self::Query) -> Result<usize, ApplicationError> {
        Ok(size_of::<Self::ReadResult>()
            + self.manifest(*query).map_or(0, |m| {
                m.retained_bytes() - size_of::<ResponsibilityManifest>()
            }))
    }
    fn read_result_bytes(
        &self,
        result: &Self::ReadResult,
        nested_limit: usize,
    ) -> Result<usize, ApplicationError> {
        let bytes = result.as_ref().map_or(0, |m| {
            m.retained_bytes() - size_of::<ResponsibilityManifest>()
        });
        if bytes > nested_limit {
            return Err(ApplicationError::ReceiptBudget);
        }
        Ok(bytes)
    }
}
impl CheckpointStateMachine for Directory {
    fn schema_version(&self) -> u64 {
        if self.reparent_guards {
            REPARENT_GUARD_DIRECTORY_SCHEMA
        } else if self.local_reparenting {
            LOCAL_REPARENT_DIRECTORY_SCHEMA
        } else if self.child_slot_retirement {
            CHILD_SLOT_DIRECTORY_SCHEMA
        } else if self.namespace_deletion {
            DELETION_DIRECTORY_SCHEMA
        } else if self.retained_insertion {
            RETAINED_INSERTION_DIRECTORY_APPLICATION_SCHEMA
        } else if self.cross_authority_insertion {
            CROSS_AUTHORITY_INSERTION_DIRECTORY_APPLICATION_SCHEMA
        } else if self.recursive_insertion {
            RECURSIVE_INSERTION_DIRECTORY_APPLICATION_SCHEMA
        } else if self.responsibility_insertion {
            INSERTION_DIRECTORY_APPLICATION_SCHEMA
        } else if self.namespace_transfers {
            NAMESPACE_TRANSFER_DIRECTORY_APPLICATION_SCHEMA
        } else if self.namespace_creation {
            NAMESPACE_DIRECTORY_APPLICATION_SCHEMA
        } else if self.group_creation {
            CREATION_DIRECTORY_APPLICATION_SCHEMA
        } else {
            DIRECTORY_APPLICATION_SCHEMA
        }
    }
    fn checkpoint(&self, max_bytes: usize) -> Result<Vec<u8>, ApplicationError> {
        // Unique commands in first-application order reconstruct manifests and
        // original outcomes. Retry/conflict/noop entries only affect applied.
        let len = 58
            + self.plan.encoded_len()
            + 28 * self.history.len()
            + self.history_bytes
            + self.control_bytes;
        if len > max_bytes {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut bytes = Vec::with_capacity(len);
        bytes.extend(if self.reparent_guards {
            b"VBDIR012"
        } else if self.local_reparenting {
            b"VBDIR011"
        } else if self.child_slot_retirement {
            b"VBDIR010"
        } else if self.namespace_deletion {
            b"VBDIR009"
        } else if self.retained_insertion {
            b"VBDIR008"
        } else if self.cross_authority_insertion {
            b"VBDIR007"
        } else if self.recursive_insertion {
            b"VBDIR006"
        } else if self.responsibility_insertion {
            b"VBDIR005"
        } else if self.namespace_transfers {
            b"VBDIR004"
        } else if self.namespace_creation {
            b"VBDIR003"
        } else if self.group_creation {
            b"VBDIR002"
        } else {
            b"VBDIR001"
        });
        bytes.extend(self.applied.to_le_bytes());
        bytes.extend((self.limits.operations as u32).to_le_bytes());
        bytes.extend((self.limits.history_bytes as u64).to_le_bytes());
        put_group(&mut bytes, self.plan.authority);
        bytes.extend((self.plan.manifests.len() as u16).to_le_bytes());
        for manifest in self.plan.manifests.values() {
            bytes.extend((manifest_len(manifest) as u32).to_le_bytes());
            put_manifest(&mut bytes, manifest);
        }
        bytes.extend((self.history.len() as u32).to_le_bytes());
        let mut records: Vec<_> = self.history.iter().collect();
        records.sort_by_key(|(_, h)| h.index);
        for (operation, history) in records {
            bytes.extend(history.index.to_le_bytes());
            bytes.extend(operation.get().to_le_bytes());
            bytes.extend((history.bytes.len() as u32).to_le_bytes());
            bytes.extend(&history.bytes);
        }
        Ok(bytes)
    }
    fn restore_checkpoint(
        &mut self,
        schema: u64,
        applied: u64,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        if schema != self.schema_version() {
            return Err(ApplicationError::UnsupportedSchema);
        }
        if bytes.len() > self.readiness_requirements().snapshot_bytes {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let restore = || -> Result<Self, ApplicationError> {
            let mut reader = Reader::new(bytes);
            if reader.take(8)?
                != if self.reparent_guards {
                    b"VBDIR012"
                } else if self.local_reparenting {
                    b"VBDIR011"
                } else if self.child_slot_retirement {
                    b"VBDIR010"
                } else if self.namespace_deletion {
                    b"VBDIR009"
                } else if self.retained_insertion {
                    b"VBDIR008"
                } else if self.cross_authority_insertion {
                    b"VBDIR007"
                } else if self.recursive_insertion {
                    b"VBDIR006"
                } else if self.responsibility_insertion {
                    b"VBDIR005"
                } else if self.namespace_transfers {
                    b"VBDIR004"
                } else if self.namespace_creation {
                    b"VBDIR003"
                } else if self.group_creation {
                    b"VBDIR002"
                } else {
                    b"VBDIR001"
                }
                || reader.u64()? != applied
                || reader.u32()? as usize != self.limits.operations
                || reader.u64()? != self.limits.history_bytes as u64
                || reader.group()? != self.plan.authority
                || usize::from(reader.u16()?) != self.plan.manifests.len()
            {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            for expected in self.plan.manifests.values() {
                let len = reader.u32()? as usize;
                if read_manifest(reader.take(len)?)? != *expected {
                    return Err(ApplicationError::InvalidCheckpoint);
                }
            }
            let count = reader.u32()? as usize;
            if count > if self.reparent_guards { 4 } else { 2 } * self.limits.operations
                || count as u64 > applied
            {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            let mut next = Self::new(self.plan.clone(), self.limits).map_err(|(e, _)| e)?;
            next.group_creation = self.group_creation;
            next.namespace_creation = self.namespace_creation;
            next.namespace_transfers = self.namespace_transfers;
            next.responsibility_insertion = self.responsibility_insertion;
            next.recursive_insertion = self.recursive_insertion;
            next.cross_authority_insertion = self.cross_authority_insertion;
            next.retained_insertion = self.retained_insertion;
            next.namespace_deletion = self.namespace_deletion;
            next.child_slot_retirement = self.child_slot_retirement;
            next.local_reparenting = self.local_reparenting;
            next.reparent_guards = self.reparent_guards;
            let mut previous = 0;
            for _ in 0..count {
                let index = reader.u64()?;
                let operation = reader.operation()?;
                let len = reader.u32()? as usize;
                if index <= previous || index > applied || next.history.contains_key(&operation) {
                    return Err(ApplicationError::InvalidCheckpoint);
                }
                let command = reader.take(len)?;
                next.execute(index, operation, command)?;
                previous = index;
            }
            if !reader.done() {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            next.applied = applied;
            Ok(next)
        };
        let next = restore().map_err(|_| ApplicationError::InvalidCheckpoint)?;
        *self = next;
        Ok(())
    }
}
