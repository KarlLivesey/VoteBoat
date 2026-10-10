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
//! Fixed ownership guard over the public application and partition-policy seams.
//! An authenticated trusted grant is committed locally before service. There is
//! no parent access in normal data execution and no transfer/import/activation.
use crate::{application::*, identity::*, log::*, routing::codec::*, routing::*};
use std::{
    collections::{BTreeMap, BTreeSet},
    mem::size_of,
};
pub(crate) mod codec;
use codec::{decode, Command, DATA_HEADER};
mod control_apply;
mod control_reads;
mod cross_parent_adoption;
mod locator_adoption;
mod metadata_adoption;
mod recovery;
pub use locator_adoption::*;
pub use metadata_adoption::{
    MetadataGrantStatus, OwnerMetadataAdoption, MAX_METADATA_ADOPTION_BYTES,
    METADATA_ADOPTING_ROUTED_SCHEMA,
};
pub(crate) mod parent_adoption;
pub(crate) mod parent_slots;
pub use codec::{encode_fence, encode_routed, encode_scope_fence};
pub use control_reads::{RoutedControlQuery, RoutedControlRead, RoutedControlReads};
pub use cross_parent_adoption::{
    CrossOwnerParentAdoption, CROSS_PARENT_ADOPTING_ROUTED_SCHEMA, MAX_CROSS_PARENT_ADOPTION_BYTES,
};
pub use parent_adoption::{
    OwnerParentAdoption, ParentGrantStatus, MAX_PARENT_ADOPTIONS, MAX_PARENT_ADOPTION_BYTES,
    PARENT_ADOPTING_ROUTED_SCHEMA,
};
use parent_adoption::{ParentAdoptionCommand, ParentAdoptionRecord};

pub const ROUTED_APPLICATION_SCHEMA: u64 = 1;
pub const SCOPED_ROUTED_APPLICATION_SCHEMA: u64 = 2;
pub const MAX_ROUTED_OPERATIONS: usize = 4096;
pub const MAX_ROUTED_SEMANTIC_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_ROUTED_PAYLOAD_BYTES: usize = 1024 * 1024;
pub const MAX_ROUTED_CHECKPOINT_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_ROUTED_COMMAND_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_ROUTED_PENDING: usize = 8192;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RoutedLimits {
    pub operations: usize,
    pub semantic_bytes: usize,
    pub payload_bytes: usize,
    pub inner_checkpoint_bytes: usize,
}
impl RoutedLimits {
    fn valid(self) -> bool {
        self.operations > 0
            && self.operations <= MAX_ROUTED_OPERATIONS
            && self.semantic_bytes > 0
            && self.semantic_bytes <= MAX_ROUTED_SEMANTIC_BYTES
            && self.payload_bytes > 0
            && self.payload_bytes <= MAX_ROUTED_PAYLOAD_BYTES
            && self.inner_checkpoint_bytes > 0
            && self.inner_checkpoint_bytes <= MAX_ROUTED_CHECKPOINT_BYTES
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OwnershipFence {
    pub group: GroupIdentity,
    pub responsibility: ResponsibilityIdentity,
    pub epoch: OwnershipEpoch,
    pub operation: OperationId,
    pub index: u64,
}
/// Original local scoped outcome, not a whole-source or transferable certificate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScopedOwnershipFence {
    pub fence: OwnershipFence,
    pub scope: BucketRange,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RoutedOutcome<R> {
    Bootstrapped,
    Fenced(OwnershipFence),
    ScopeFenced(ScopedOwnershipFence),
    GrantAdopted(RetainedGrantStatus),
    ParentAdopted(ParentGrantStatus),
    Applied(R),
    Rejected(RoutingError),
    OperationConflict,
}
/// Original local committed retained-grant outcome; remote use requires quorum provenance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetainedGrantStatus {
    pub operation: OperationId,
    pub index: u64,
    pub transfer: OperationId,
    pub epoch: OwnershipEpoch,
    pub generation: RouteGeneration,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoutedReceipt<R> {
    pub index: u64,
    pub operation: OperationId,
    pub outcome: RoutedOutcome<R>,
}
impl<R: ApplicationReceipt> ApplicationReceipt for RoutedReceipt<R> {
    fn index(&self) -> u64 {
        self.index
    }
    fn operation(&self) -> OperationId {
        self.operation
    }
    fn nested_bytes(&self, limit: usize) -> Result<usize, ApplicationError> {
        match &self.outcome {
            RoutedOutcome::Applied(r) => r.nested_bytes(limit),
            _ => Ok(0),
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoutedQuery<Q> {
    pub hint: RouteHint,
    pub key: Vec<u8>,
    pub query: Q,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RoutedRead<R> {
    Served(R),
    Rejected(RoutingError),
}
#[derive(Clone)]
struct Semantic {
    index: u64,
    key: Vec<u8>,
    payload: Vec<u8>,
}
pub struct RoutedRejected<A, P> {
    pub error: ApplicationError,
    pub grant: ResponsibilityManifest,
    pub application: A,
    pub policy: P,
}
#[derive(Clone)]
pub struct RoutedApplication<A, P> {
    local: GroupIdentity,
    grant: ResponsibilityManifest,
    inner: A,
    policy: P,
    limits: RoutedLimits,
    binding: Vec<u8>,
    initialized: Option<(OperationId, u64)>,
    fence: Option<OwnershipFence>,
    scope_limit: usize,
    scope_fences: Vec<ScopedOwnershipFence>,
    history: BTreeMap<OperationId, Semantic>,
    semantic_bytes: usize,
    parent_adoption_limit: usize,
    cross_parent_adoption: bool,
    metadata_adoption: bool,
    metadata_locator_adoption: bool,
    parent_adoptions: Vec<ParentAdoptionRecord>,
}
impl<A: CheckpointStateMachine, P: PartitionPolicy + Clone> RoutedApplication<A, P> {
    #[allow(clippy::result_large_err)] // Return the original owned application and grant.
    pub fn new(
        local: GroupIdentity,
        grant: ResponsibilityManifest,
        application: A,
        policy: P,
        limits: RoutedLimits,
    ) -> Result<Self, RoutedRejected<A, P>> {
        let construct = || -> Result<Vec<u8>, ApplicationError> {
            application.validate_group(local)?;
            if !limits.valid()
                || application.applied_index() != 0
                || grant.input().state != ResponsibilityState::Active
                || policy.scheme() != grant.input().scheme
            {
                return Err(ApplicationError::InvalidCommand);
            }
            let owns = match &grant.input().execution {
                ExecutionMode::Single(group) => *group == local,
                ExecutionMode::Partitioned(v) | ExecutionMode::Delegated(v) => v
                    .iter()
                    .any(|entry| entry.target == RouteTarget::Group(local)),
            };
            if !owns {
                return Err(ApplicationError::InvalidCommand);
            }
            let initial = application.checkpoint(limits.inner_checkpoint_bytes)?;
            let len = 68 + manifest_len(&grant) + initial.len();
            if len > MAX_ROUTED_COMMAND_BYTES {
                return Err(ApplicationError::InvalidCommand);
            }
            let mut binding = Vec::with_capacity(len);
            binding.extend(b"VBROWN01");
            put_group(&mut binding, local);
            binding.extend((limits.operations as u32).to_le_bytes());
            binding.extend((limits.semantic_bytes as u64).to_le_bytes());
            binding.extend((limits.payload_bytes as u32).to_le_bytes());
            binding.extend((limits.inner_checkpoint_bytes as u32).to_le_bytes());
            binding.extend((manifest_len(&grant) as u32).to_le_bytes());
            put_manifest(&mut binding, &grant);
            binding.extend(application.schema_version().to_le_bytes());
            binding.extend((initial.len() as u32).to_le_bytes());
            binding.extend(initial);
            Ok(binding)
        };
        let binding = match construct() {
            Ok(binding) => binding,
            Err(error) => {
                return Err(RoutedRejected {
                    error,
                    grant,
                    application,
                    policy,
                })
            }
        };
        Ok(Self {
            local,
            grant,
            inner: application,
            policy,
            limits,
            binding,
            initialized: None,
            fence: None,
            scope_limit: 0,
            scope_fences: Vec::new(),
            history: BTreeMap::new(),
            semantic_bytes: 0,
            parent_adoption_limit: 0,
            cross_parent_adoption: false,
            metadata_adoption: false,
            metadata_locator_adoption: false,
            parent_adoptions: Vec::new(),
        })
    }
    /// Select schema2 before bootstrap. Retains ownership of the original object
    /// on rejection. No live profile upgrade or unfencing operation exists.
    #[allow(clippy::result_large_err)]
    pub fn with_scoped_fencing(mut self, maximum: usize) -> Result<Self, (ApplicationError, Self)> {
        if maximum == 0
            || maximum > MAX_MANIFEST_ROUTES
            || self.scope_limit != 0
            || self.parent_adoption_limit != 0
            || self.inner.applied_index() != 0
            || self.initialized.is_some()
            || self.fence.is_some()
            || self
                .binding
                .len()
                .checked_add(2)
                .is_none_or(|n| n > MAX_ROUTED_COMMAND_BYTES)
        {
            return Err((ApplicationError::InvalidCommand, self));
        }
        self.scope_limit = maximum;
        self.binding[..8].copy_from_slice(b"VBROWN02");
        self.binding.extend((maximum as u16).to_le_bytes());
        Ok(self)
    }
    /// Local diagnostics only; remote evidence requires the original quorum outcome.
    pub fn scoped_fence_limit(&self) -> usize {
        self.scope_limit
    }
    pub fn scoped_fences(&self) -> &[ScopedOwnershipFence] {
        &self.scope_fences
    }
    fn scoped_operation(&self, operation: OperationId) -> bool {
        self.scope_fences
            .iter()
            .any(|s| s.fence.operation == operation)
    }
    fn owns_scope(&self, scope: BucketRange) -> bool {
        let m = self.grant.input();
        if scope.start() < m.scope.start() || scope.end() > m.scope.end() {
            return false;
        }
        match &m.execution {
            ExecutionMode::Single(g) => *g == self.local,
            ExecutionMode::Partitioned(v) | ExecutionMode::Delegated(v) => v.iter().any(|r| {
                r.target == RouteTarget::Group(self.local)
                    && r.scope.start() <= scope.start()
                    && r.scope.end() >= scope.end()
            }),
        }
    }
    pub fn bootstrap_command(&self, max_bytes: usize) -> Result<Vec<u8>, ApplicationError> {
        if self.binding.len() > max_bytes {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(self.binding.clone())
    }
    pub fn is_initialized(&self) -> bool {
        self.initialized.is_some()
    }
    pub(crate) fn initialization(&self) -> Option<(OperationId, u64)> {
        self.initialized
    }
    pub(crate) fn has_data_operation(&self, operation: OperationId) -> bool {
        self.history.contains_key(&operation)
    }
    pub(crate) fn has_semantic_index(&self, index: u64) -> bool {
        self.history.values().any(|s| s.index == index)
            || self.initialized.is_some_and(|(_, i)| i == index)
            || self.scope_fences.iter().any(|f| f.fence.index == index)
            || self.fence.is_some_and(|f| f.index == index)
            || self
                .parent_adoptions
                .iter()
                .any(|a| a.status.index == index)
    }
    pub fn fence(&self) -> Option<OwnershipFence> {
        self.fence
    }
    pub fn grant(&self) -> &ResponsibilityManifest {
        &self.grant
    }
    pub fn local(&self) -> GroupIdentity {
        self.local
    }
    pub fn limits(&self) -> RoutedLimits {
        self.limits
    }
    /// Local diagnostic access; serving reads must use RoutedQuery/read barriers.
    pub fn application(&self) -> &A {
        &self.inner
    }
    pub fn remaining_operations(&self) -> usize {
        self.limits.operations - self.history.len()
    }
    pub fn readiness_requirements(&self) -> crate::raft::ReadinessRequirements {
        crate::raft::ReadinessRequirements {
            application_schema: if self.metadata_locator_adoption {
                LOCATOR_ADOPTING_ROUTED_SCHEMA
            } else if self.metadata_adoption {
                METADATA_ADOPTING_ROUTED_SCHEMA
            } else if self.cross_parent_adoption {
                CROSS_PARENT_ADOPTING_ROUTED_SCHEMA
            } else if self.parent_adoption_limit != 0 {
                PARENT_ADOPTING_ROUTED_SCHEMA
            } else if self.scope_limit == 0 {
                ROUTED_APPLICATION_SCHEMA
            } else {
                SCOPED_ROUTED_APPLICATION_SCHEMA
            },
            command_bytes: self
                .binding
                .len()
                .max(DATA_HEADER + MAX_ROUTING_KEY_BYTES + self.limits.payload_bytes)
                .max(if self.parent_adoption_limit == 0 {
                    0
                } else {
                    self.parent_command_bound()
                }),
            snapshot_bytes: 94
                + self.binding.len()
                + self.limits.inner_checkpoint_bytes
                + 30 * self.limits.operations
                + self.limits.semantic_bytes
                + if self.scope_limit == 0 {
                    0
                } else {
                    2 + 36 * self.scope_limit
                }
                + if self.parent_adoption_limit == 0 {
                    0
                } else {
                    2 + self.parent_adoption_limit * (28 + self.parent_command_bound())
                },
        }
    }
    pub fn check_context(&self, hint: &RouteHint, key: &[u8]) -> Result<(), RoutingError> {
        self.check_grant_context(&self.grant, hint, key)
    }
    pub(crate) fn check_grant_context(
        &self,
        grant: &ResponsibilityManifest,
        hint: &RouteHint,
        key: &[u8],
    ) -> Result<(), RoutingError> {
        if !self.is_initialized() {
            return Err(RoutingError::WrongOwner);
        }
        if self.fence.is_some() {
            return Err(RoutingError::Fenced);
        }
        check_owner(grant, self.local, hint, key, &self.policy)?;
        if self
            .scope_fences
            .iter()
            .any(|s| s.scope.contains(hint.bucket))
        {
            return Err(RoutingError::Fenced);
        }
        Ok(())
    }
    pub(crate) fn original_hint(&self, mut hint: RouteHint) -> Result<RouteHint, RoutingError> {
        let route = self.grant.select(hint.bucket)?;
        hint.scope = route.scope;
        hint.epoch = self.grant.input().epoch;
        hint.generation = self.grant.input().generation;
        Ok(hint)
    }
    fn request<'a>(&self, bytes: &'a [u8]) -> Result<Command<'a>, ApplicationError> {
        let request = decode(bytes, self.limits.payload_bytes)?;
        if matches!(request, Command::ScopeFence(_, _)) && self.scope_limit == 0 {
            return Err(ApplicationError::InvalidCommand);
        }
        if matches!(request, Command::ParentAdopt(_)) && self.parent_adoption_limit == 0 {
            return Err(ApplicationError::InvalidCommand);
        }
        if let Command::Bootstrap(binding) = request {
            if binding != self.binding {
                return Err(ApplicationError::InvalidCommand);
            }
        } else if !self.is_initialized() {
            return Err(ApplicationError::NotApplied);
        }
        Ok(request)
    }
    fn semantic_conflict(&self, operation: OperationId, key: &[u8], payload: &[u8]) -> bool {
        self.initialized.is_some_and(|(op, _)| op == operation)
            || self.parent_operation(operation)
            || self.scoped_operation(operation)
            || self.fence.is_some_and(|f| f.operation == operation)
            || self
                .history
                .get(&operation)
                .is_some_and(|old| old.key != key || old.payload != payload)
    }
    fn initial_key(&self, key: &[u8]) -> bool {
        let Ok(bucket) = self.policy.bucket(key) else {
            return false;
        };
        if bucket >= 256
            || key.len() > MAX_ROUTING_KEY_BYTES
            || !self.grant.input().scope.contains(bucket)
        {
            return false;
        }
        match &self.grant.input().execution {
            ExecutionMode::Single(g) => *g == self.local,
            ExecutionMode::Partitioned(v) | ExecutionMode::Delegated(v) => v
                .iter()
                .any(|e| e.scope.contains(bucket) && e.target == RouteTarget::Group(self.local)),
        }
    }
}
impl<A, P> StateMachine for RoutedApplication<A, P>
where
    A: CheckpointStateMachine + BoundedStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    type Receipt = RoutedReceipt<A::Receipt>;
    fn validate_group(&self, group: GroupIdentity) -> Result<(), ApplicationError> {
        if group != self.local {
            return Err(ApplicationError::InvalidCommand);
        }
        self.inner.validate_group(group)
    }
    fn deployment_requirements(&self) -> Option<crate::raft::ReadinessRequirements> {
        Some(self.readiness_requirements())
    }
    fn applied_index(&self) -> u64 {
        self.inner.applied_index()
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
            if next.applied_index().checked_add(1) != Some(entry.index) {
                return Err(ApplicationError::IndexGap);
            }
            let mut projected = entry.clone();
            let mut outcome = None;
            let mut new_semantic = None;
            if let EntryPayload::Command { operation, bytes } = &entry.payload {
                projected.payload = EntryPayload::Noop;
                let result = match next.request(bytes)? {
                    Command::Bootstrap(_) => {
                        Some(next.apply_bootstrap_control(*operation, entry.index))
                    }
                    Command::Fence(epoch) => {
                        Some(next.apply_full_fence(*operation, entry.index, epoch))
                    }
                    Command::ScopeFence(epoch, scope) => {
                        Some(next.apply_scope_fence(*operation, entry.index, epoch, scope)?)
                    }
                    Command::ParentAdopt(bytes) => {
                        Some(next.apply_parent_adoption(*operation, entry.index, bytes)?)
                    }
                    Command::Data { hint, key, payload } => {
                        if let Err(error) = next.check_context(&hint, key) {
                            Some(RoutedOutcome::Rejected(error))
                        } else if next.semantic_conflict(*operation, key, payload) {
                            Some(RoutedOutcome::OperationConflict)
                        } else {
                            if !next.history.contains_key(operation) {
                                if next.history.len() == next.limits.operations
                                    || next.semantic_bytes + key.len() + payload.len()
                                        > next.limits.semantic_bytes
                                {
                                    return Err(ApplicationError::DedupCapacity);
                                }
                                let semantic = Semantic {
                                    index: entry.index,
                                    key: key.to_vec(),
                                    payload: payload.to_vec(),
                                };
                                if next.semantic_bytes
                                    + semantic.key.capacity()
                                    + semantic.payload.capacity()
                                    > next.limits.semantic_bytes
                                {
                                    return Err(ApplicationError::DedupCapacity);
                                }
                                new_semantic = Some((*operation, semantic));
                            }
                            projected.payload = EntryPayload::Command {
                                operation: *operation,
                                bytes: payload.to_vec(),
                            };
                            // Only the selected application's checked receipt supplies success.
                            None
                        }
                    }
                };
                outcome = Some((*operation, result));
            }
            let applied = next.inner.apply_batch(std::slice::from_ref(&projected))?;
            if next.inner.applied_index() != entry.index {
                return Err(ApplicationError::InvalidCommand);
            }
            if let Some((operation, mut result)) = outcome {
                if matches!(projected.payload, EntryPayload::Command { .. }) {
                    let mut applied = applied.into_iter();
                    let receipt = applied.next().ok_or(ApplicationError::InvalidCommand)?;
                    if applied.next().is_some()
                        || receipt.index() != entry.index
                        || receipt.operation() != operation
                    {
                        return Err(ApplicationError::InvalidCommand);
                    }
                    result = Some(RoutedOutcome::Applied(receipt));
                } else if !applied.is_empty() {
                    return Err(ApplicationError::InvalidCommand);
                }
                if let Some((operation, semantic)) = new_semantic {
                    next.semantic_bytes += semantic.key.capacity() + semantic.payload.capacity();
                    next.history.insert(operation, semantic);
                }
                receipts.push(RoutedReceipt {
                    index: entry.index,
                    operation,
                    outcome: result.ok_or(ApplicationError::InvalidCommand)?,
                });
            } else if !applied.is_empty() {
                return Err(ApplicationError::InvalidCommand);
            }
        }
        *self = next;
        Ok(receipts)
    }
}
impl<A, P> BoundedStateMachine for RoutedApplication<A, P>
where
    A: CheckpointStateMachine + BoundedStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        if self.scope_limit != 0 || self.parent_adoption_limit != 0 {
            let mut next = self.clone();
            let receipts = next.apply_batch(entries)?;
            let mut bound = receipts
                .len()
                .checked_mul(size_of::<Self::Receipt>())
                .ok_or(ApplicationError::ReceiptBudget)?;
            for receipt in receipts {
                bound = bound
                    .checked_add(receipt.nested_bytes(usize::MAX)?)
                    .ok_or(ApplicationError::ReceiptBudget)?;
            }
            return Ok(bound);
        }
        let mut projected = Vec::with_capacity(entries.len());
        let mut commands = 0usize;
        let mut data = 0usize;
        for entry in entries {
            let mut inner = entry.clone();
            if let EntryPayload::Command { operation, bytes } = &entry.payload {
                commands += 1;
                inner.payload = match self.request(bytes)? {
                    Command::Data { payload, .. } => {
                        data += 1;
                        EntryPayload::Command {
                            operation: *operation,
                            bytes: payload.to_vec(),
                        }
                    }
                    _ => EntryPayload::Noop,
                };
            }
            projected.push(inner);
        }
        let inner = self.inner.receipt_bytes_bound(&projected)?;
        let nested = inner
            .checked_sub(
                data.checked_mul(size_of::<A::Receipt>())
                    .ok_or(ApplicationError::ReceiptBudget)?,
            )
            .ok_or(ApplicationError::ReceiptBudget)?;
        commands
            .checked_mul(size_of::<Self::Receipt>())
            .and_then(|n| n.checked_add(nested))
            .ok_or(ApplicationError::ReceiptBudget)
    }
}
impl<A, P> ProposalAdmission for RoutedApplication<A, P>
where
    A: CheckpointStateMachine + BoundedStateMachine + ProposalAdmission,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    fn validate_proposal<'a>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError> {
        if self.scope_limit != 0 || self.parent_adoption_limit != 0 {
            return self.validate_replayed_proposal(operation, bytes, pending);
        }
        let candidate = self.request(bytes)?;
        let mut reserved = BTreeMap::new();
        let mut retained = 0;
        let mut projected = Vec::new();
        let mut reserve = |id, key: &[u8], payload: &[u8]| -> Result<(), ApplicationError> {
            if !self.history.contains_key(&id) {
                let size = reserved.entry(id).or_insert(0);
                let bytes = key.len() + payload.len();
                if bytes > *size {
                    retained += bytes - *size;
                    *size = bytes;
                }
                if reserved.len() > self.remaining_operations()
                    || retained > self.limits.semantic_bytes - self.semantic_bytes
                {
                    return Err(ApplicationError::DedupCapacity);
                }
            }
            Ok(())
        };
        for (position, (id, request)) in pending.enumerate() {
            if position >= MAX_ROUTED_PENDING {
                return Err(ApplicationError::ReceiptBudget);
            }
            if let Command::Data { key, payload, .. } = self.request(request)? {
                reserve(id, key, payload)?;
                projected.push((id, payload));
            }
        }
        let bound = match candidate {
            Command::Data { hint, key, payload } => {
                self.check_context(&hint, key)
                    .map_err(|_| ApplicationError::InvalidCommand)?;
                if self.semantic_conflict(operation, key, payload) {
                    return Err(ApplicationError::InvalidCommand);
                }
                reserve(operation, key, payload)?;
                let bound =
                    self.inner
                        .validate_proposal(operation, payload, projected.into_iter())?;
                size_of::<Self::Receipt>()
                    .checked_add(
                        bound
                            .checked_sub(size_of::<A::Receipt>())
                            .ok_or(ApplicationError::ReceiptBudget)?,
                    )
                    .ok_or(ApplicationError::ReceiptBudget)?
            }
            Command::Bootstrap(_) => {
                if self.initialized.is_some_and(|(op, _)| op != operation)
                    || self.history.contains_key(&operation)
                    || self.fence.is_some_and(|f| f.operation == operation)
                {
                    return Err(ApplicationError::InvalidCommand);
                }
                size_of::<Self::Receipt>()
            }
            Command::ScopeFence(_, _) => return Err(ApplicationError::InvalidCommand),
            Command::ParentAdopt(_) => return Err(ApplicationError::InvalidCommand),
            Command::Fence(epoch) => {
                if epoch != self.grant.input().epoch
                    || self.history.contains_key(&operation)
                    || self.initialized.is_some_and(|(op, _)| op == operation)
                    || self.fence.is_some_and(|f| f.operation != operation)
                {
                    return Err(ApplicationError::InvalidCommand);
                }
                size_of::<Self::Receipt>()
            }
        };
        Ok(bound)
    }
}
impl<A, P> ReadableStateMachine for RoutedApplication<A, P>
where
    A: CheckpointStateMachine + BoundedStateMachine + ReadableStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    type Query = RoutedQuery<A::Query>;
    type ReadResult = RoutedRead<A::ReadResult>;
    fn read_at(
        &self,
        index: u64,
        query: Self::Query,
    ) -> Result<Self::ReadResult, ApplicationError> {
        if index > self.applied_index() {
            return Err(ApplicationError::NotApplied);
        }
        if let Err(error) = self.check_context(&query.hint, &query.key) {
            return Ok(RoutedRead::Rejected(error));
        }
        self.inner
            .read_at(index, query.query)
            .map(RoutedRead::Served)
    }
}
impl<A, P> BoundedReadableStateMachine for RoutedApplication<A, P>
where
    A: CheckpointStateMachine + BoundedStateMachine + BoundedReadableStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    fn query_bytes(&self, query: &Self::Query, limit: usize) -> Result<usize, ApplicationError> {
        if query.key.capacity() > MAX_ROUTING_KEY_BYTES || query.key.capacity() > limit {
            return Err(ApplicationError::ReceiptBudget);
        }
        let nested = self
            .inner
            .query_bytes(&query.query, limit - query.key.capacity())?;
        query
            .key
            .capacity()
            .checked_add(nested)
            .filter(|n| *n <= limit)
            .ok_or(ApplicationError::ReceiptBudget)
    }
    fn read_result_bound(&self, query: &Self::Query) -> Result<usize, ApplicationError> {
        let inner = self.inner.read_result_bound(&query.query)?;
        size_of::<Self::ReadResult>()
            .checked_add(
                inner
                    .checked_sub(size_of::<A::ReadResult>())
                    .ok_or(ApplicationError::ReceiptBudget)?,
            )
            .ok_or(ApplicationError::ReceiptBudget)
    }
    fn read_result_bytes(
        &self,
        result: &Self::ReadResult,
        limit: usize,
    ) -> Result<usize, ApplicationError> {
        match result {
            RoutedRead::Served(inner) => self.inner.read_result_bytes(inner, limit),
            RoutedRead::Rejected(_) => Ok(0),
        }
    }
}

impl<A, P> CheckpointStateMachine for RoutedApplication<A, P>
where
    A: CheckpointStateMachine + BoundedStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    fn schema_version(&self) -> u64 {
        if self.metadata_locator_adoption {
            LOCATOR_ADOPTING_ROUTED_SCHEMA
        } else if self.metadata_adoption {
            METADATA_ADOPTING_ROUTED_SCHEMA
        } else if self.cross_parent_adoption {
            CROSS_PARENT_ADOPTING_ROUTED_SCHEMA
        } else if self.parent_adoption_limit != 0 {
            PARENT_ADOPTING_ROUTED_SCHEMA
        } else if self.scope_limit == 0 {
            ROUTED_APPLICATION_SCHEMA
        } else {
            SCOPED_ROUTED_APPLICATION_SCHEMA
        }
    }
    fn checkpoint(&self, max_bytes: usize) -> Result<Vec<u8>, ApplicationError> {
        let inner = self.inner.checkpoint(self.limits.inner_checkpoint_bytes)?;
        let semantic: usize = self
            .history
            .values()
            .map(|s| s.key.len() + s.payload.len())
            .sum();
        let len = 38
            + self.binding.len()
            + inner.len()
            + self.initialized.map_or(0, |_| 24)
            + self.fence.map_or(0, |_| 32)
            + 30 * self.history.len()
            + semantic
            + if self.scope_limit == 0 {
                0
            } else {
                2 + 36 * self.scope_fences.len()
            }
            + if self.parent_adoption_limit == 0 {
                0
            } else {
                2 + self
                    .parent_adoptions
                    .iter()
                    .map(|a| {
                        28 + a
                            .command
                            .encode(self.parent_command_bound())
                            .expect("checked command")
                            .len()
                    })
                    .sum::<usize>()
            };
        if len > max_bytes {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut bytes = Vec::with_capacity(len);
        bytes.extend(if self.metadata_locator_adoption {
            b"VBROUT06"
        } else if self.metadata_adoption {
            b"VBROUT05"
        } else if self.cross_parent_adoption {
            b"VBROUT04"
        } else if self.parent_adoption_limit != 0 {
            b"VBROUT03"
        } else if self.scope_limit == 0 {
            b"VBROUT01"
        } else {
            b"VBROUT02"
        });
        bytes.extend(self.applied_index().to_le_bytes());
        bytes.extend((self.binding.len() as u32).to_le_bytes());
        bytes.extend(&self.binding);
        bytes.push(u8::from(self.initialized.is_some()));
        if let Some((operation, index)) = self.initialized {
            bytes.extend(operation.get().to_le_bytes());
            bytes.extend(index.to_le_bytes());
        }
        bytes.push(u8::from(self.fence.is_some()));
        if let Some(fence) = self.fence {
            bytes.extend(fence.operation.get().to_le_bytes());
            bytes.extend(fence.index.to_le_bytes());
            bytes.extend(fence.epoch.get().to_le_bytes());
        }
        if self.scope_limit != 0 {
            bytes.extend((self.scope_fences.len() as u16).to_le_bytes());
            for s in &self.scope_fences {
                bytes.extend(s.fence.operation.get().to_le_bytes());
                bytes.extend(s.fence.index.to_le_bytes());
                bytes.extend(s.fence.epoch.get().to_le_bytes());
                put_range(&mut bytes, s.scope);
            }
        }
        bytes.extend(self.inner.schema_version().to_le_bytes());
        bytes.extend((inner.len() as u32).to_le_bytes());
        bytes.extend(inner);
        bytes.extend((self.history.len() as u32).to_le_bytes());
        for (operation, semantic) in &self.history {
            bytes.extend(operation.get().to_le_bytes());
            bytes.extend(semantic.index.to_le_bytes());
            bytes.extend((semantic.key.len() as u16).to_le_bytes());
            bytes.extend((semantic.payload.len() as u32).to_le_bytes());
            bytes.extend(&semantic.key);
            bytes.extend(&semantic.payload);
        }
        if self.parent_adoption_limit != 0 {
            bytes.extend((self.parent_adoptions.len() as u16).to_le_bytes());
            for a in &self.parent_adoptions {
                let command = a.command.encode(self.parent_command_bound())?;
                bytes.extend(a.status.operation.get().to_le_bytes());
                bytes.extend(a.status.index.to_le_bytes());
                bytes.extend((command.len() as u32).to_le_bytes());
                bytes.extend(command);
            }
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
        let next = self
            .decode_checkpoint(applied, bytes)
            .map_err(|_| ApplicationError::InvalidCheckpoint)?;
        *self = next;
        Ok(())
    }
}
