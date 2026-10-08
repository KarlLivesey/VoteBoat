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
//! Replicated static-ownership directory application over ordinary Raft entries.
//!
//! Initial assignments are an explicit trusted bootstrap plan. Publish commands
//! cannot transfer ownership, fence a source or activate a target. Host-controlled
//! authorization remains required before proposal. Durable progress comes solely
//! from the existing Raft/WAL/application/checkpoint contracts.
use crate::routing::codec::*;
use crate::transfer::{TransferIntent, TransferIntentStatus, MAX_TRANSFER_INTENT_BYTES};
use crate::{application::*, identity::*, log::*, routing::*};
use std::{
    collections::{BTreeMap, BTreeSet},
    mem::size_of,
};

pub const DIRECTORY_APPLICATION_SCHEMA: u64 = 1;
pub const MAX_DIRECTORY_MANIFESTS: usize = 256;
pub const MAX_DIRECTORY_OPERATIONS: usize = 4096;
pub const MAX_DIRECTORY_HISTORY_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_DIRECTORY_PUBLICATION_BYTES: usize = 32768;
pub const MAX_DIRECTORY_COMMAND_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_DIRECTORY_PENDING: usize = 8192;

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
    Published(RouteGeneration),
    GenerationMismatch,
    UnknownResponsibility,
    OwnershipChange,
    OperationConflict,
    TransferIntentRecorded,
    LifecycleBusy,
    TransferGroupBusy,
}
#[allow(clippy::large_enum_variant)] // Bounded cold parsing; no extra heap indirection.
enum Request {
    Bootstrap,
    Publish(DirectoryCommand),
    Transfer(TransferIntent),
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
    manifests: BTreeMap<ResponsibilityIdentity, ResponsibilityManifest>,
    history: BTreeMap<OperationId, History>,
    history_bytes: usize,
    /// Locks reference existing bounded history rather than duplicate manifests.
    transfers: BTreeMap<ResponsibilityIdentity, OperationId>,
    transfer_targets: BTreeSet<GroupIdentity>,
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
            manifests: BTreeMap::new(),
            history: BTreeMap::new(),
            history_bytes: 0,
            transfers: BTreeMap::new(),
            transfer_targets: BTreeSet::new(),
        })
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
        self.limits.operations - self.history.len()
    }
    /// Commit this exact plan/capacity binding before any publication. It uses
    /// an ordinary operation ID and retains its original request/result. Large
    /// plans require explicitly compatible log/transport command envelopes.
    pub fn bootstrap_command(&self, max_bytes: usize) -> Result<Vec<u8>, ApplicationError> {
        let len = 46 + self.plan.encoded_len();
        if len > max_bytes || len > MAX_DIRECTORY_COMMAND_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut bytes = Vec::with_capacity(len);
        bytes.extend(b"VBDINIT1");
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
        if bytes.starts_with(b"VBDINIT1") {
            if bytes != self.bootstrap_command(bytes.len())? {
                return Err(ApplicationError::InvalidCommand);
            }
            Ok(Request::Bootstrap)
        } else {
            if !self.initialized {
                return Err(ApplicationError::NotApplied);
            }
            if bytes.starts_with(b"VBTINT01") {
                TransferIntent::decode(bytes).map(Request::Transfer)
            } else {
                DirectoryCommand::decode(bytes).map(Request::Publish)
            }
        }
    }
    pub fn readiness_requirements(&self) -> crate::raft::ReadinessRequirements {
        crate::raft::ReadinessRequirements {
            application_schema: DIRECTORY_APPLICATION_SCHEMA,
            command_bytes: MAX_TRANSFER_INTENT_BYTES.max(46 + self.plan.encoded_len()),
            snapshot_bytes: 58
                + self.plan.encoded_len()
                + 28 * self.limits.operations
                + self.limits.history_bytes,
        }
    }
    fn publish(&mut self, command: DirectoryCommand) -> DirectoryOutcome {
        let input = command.manifest.input();
        let id = input.responsibility;
        if self.transfers.contains_key(&id) {
            return DirectoryOutcome::LifecycleBusy;
        }
        let Some(grant) = self.plan.manifests.get(&id) else {
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
    fn begin_transfer(
        &mut self,
        operation: OperationId,
        intent: TransferIntent,
    ) -> DirectoryOutcome {
        let before = intent.before().input();
        if before.authority != self.plan.authority
            || !self.plan.manifests.contains_key(&before.responsibility)
        {
            return DirectoryOutcome::UnknownResponsibility;
        }
        if self.transfers.contains_key(&before.responsibility) {
            return DirectoryOutcome::LifecycleBusy;
        }
        if self.manifests.get(&before.responsibility) != Some(intent.before()) {
            return DirectoryOutcome::GenerationMismatch;
        }
        let targets = intent.targets();
        for target in &targets {
            let RouteTarget::Group(group) = target.target else {
                unreachable!("checked intent")
            };
            if self.transfer_targets.contains(&group)
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
            if self.history.len() == self.limits.operations
                || self.history_bytes + bytes.len() > self.limits.history_bytes
            {
                return Err(ApplicationError::DedupCapacity);
            }
            let outcome = match command {
                Request::Publish(command) => self.publish(command),
                Request::Transfer(intent) => self.begin_transfer(operation, intent),
                Request::Bootstrap => {
                    self.initialized = true;
                    DirectoryOutcome::Initialized
                }
            };
            self.history.insert(
                operation,
                History {
                    index,
                    bytes: bytes.to_vec(),
                    outcome,
                },
            );
            self.history_bytes += bytes.len();
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
        let mut reserved = BTreeMap::new();
        let mut reserve_bytes = 0;
        let mut reserve = |id, request: &[u8]| -> Result<(), ApplicationError> {
            self.request(request)?;
            if !self.history.contains_key(&id) {
                let size = reserved.entry(id).or_insert(0);
                if request.len() > *size {
                    reserve_bytes += request.len() - *size;
                    *size = request.len();
                }
                if reserved.len() > self.remaining_operations()
                    || reserve_bytes > self.limits.history_bytes - self.history_bytes
                {
                    return Err(ApplicationError::DedupCapacity);
                }
            }
            Ok(())
        };
        for (count, (id, request)) in pending.enumerate() {
            if count == MAX_DIRECTORY_PENDING {
                return Err(ApplicationError::DedupCapacity);
            }
            reserve(id, request)?;
        }
        reserve(operation, bytes)?;
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
        DIRECTORY_APPLICATION_SCHEMA
    }
    fn checkpoint(&self, max_bytes: usize) -> Result<Vec<u8>, ApplicationError> {
        // Unique commands in first-application order reconstruct manifests and
        // original outcomes. Retry/conflict/noop entries only affect applied.
        let len = 58 + self.plan.encoded_len() + 28 * self.history.len() + self.history_bytes;
        if len > max_bytes {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut bytes = Vec::with_capacity(len);
        bytes.extend(b"VBDIR001");
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
            if reader.take(8)? != b"VBDIR001"
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
            if count > self.limits.operations || count as u64 > applied {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            let mut next = Self::new(self.plan.clone(), self.limits).map_err(|(e, _)| e)?;
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
