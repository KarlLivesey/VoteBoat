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
//! Immutable exact-fence exports while unfenced source scopes continue serving.
use crate::{
    application::*,
    identity::*,
    log::*,
    routed::{
        codec::{decode, Command},
        *,
    },
    routing::{codec::Reader, *},
    scope::*,
    transfer::{ContentDigest, TransferIntent, MAX_TRANSFER_INTENT_BYTES},
    transfer_publication::*,
};
use std::{collections::BTreeMap, mem::size_of};
mod adoption;
use adoption::{Adoption, GrantChange};
mod parent;
pub use crate::routed::parent_slots::{
    CrossParentSlotAdoption, ParentSlotAdoption, ReparentSide, MAX_PARENT_SLOT_ADOPTION_BYTES,
    PARENT_SLOT_SCOPED_TRANSFER_SOURCE_SCHEMA,
};
pub use adoption::{RetainedGrantAdoption, MAX_RETAINED_ADOPTION_BYTES};
use parent::parent_command;
pub use parent::{
    LOCATOR_SCOPED_TRANSFER_SOURCE_SCHEMA, METADATA_SCOPED_TRANSFER_SOURCE_SCHEMA,
    PARENT_SCOPED_TRANSFER_SOURCE_SCHEMA,
};
pub const SCOPED_TRANSFER_SOURCE_SCHEMA: u64 = 1;
pub const BOUND_SCOPED_TRANSFER_SOURCE_SCHEMA: u64 = 2;
pub const RETAINED_SCOPED_TRANSFER_SOURCE_SCHEMA: u64 = 3;
pub const MAX_SCOPED_SOURCE_CHECKPOINT_BYTES: usize = 64 * 1024 * 1024;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScopedExportStatus {
    pub fence: ScopedOwnershipFence,
    pub digest: ContentDigest,
    pub schema: u64,
    pub payload_bytes: usize,
    pub intent_digest: Option<ContentDigest>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScopedSourceQuery<Q> {
    Data(RoutedQuery<Q>),
    Frozen(OperationId),
    Grant(OperationId),
    ParentAdoption(OperationId),
    MetadataAdoption(OperationId),
    MetadataLocatorAdoption(OperationId),
}
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(clippy::large_enum_variant)] // Fixed observation is charged by read_result_bound.
pub enum ScopedSourceRead<R> {
    Data(RoutedRead<R>),
    Frozen(Option<ScopedExportStatus>),
    Grant(Option<RetainedGrantStatus>),
    ParentAdoption(Option<ParentGrantStatus>),
    MetadataAdoption(Option<MetadataGrantStatus>),
    MetadataLocatorAdoption(Option<MetadataLocatorGrantStatus>),
}
#[derive(Clone)]
struct FrozenExport {
    digest: ContentDigest,
    image: ScopeImage,
    reservation: usize,
    intent: Option<TransferIntent>,
    intent_digest: Option<ContentDigest>,
}
/// Owns one routed application and bounded immutable provider images. No I/O or
/// additional persistence owner. Authorize fences separately from ordinary data;
/// status and exports alone never publish ownership or activate a target.
#[derive(Clone)]
pub struct ScopedTransferSource<A, P> {
    routed: RoutedApplication<A, P>,
    export_bytes: usize,
    images: BTreeMap<OperationId, FrozenExport>,
    bound_intents: bool,
    retained_grants: bool,
    active_grant: ResponsibilityManifest,
    adoptions: Vec<GrantChange>,
    parent_limit: usize,
    parent_slots: bool,
    metadata_adoption: bool,
    metadata_locator_adoption: bool,
}
impl<A, P> ScopedTransferSource<A, P>
where
    A: ScopeStateMachine + BoundedStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    #[allow(clippy::result_large_err)]
    pub fn new(
        routed: RoutedApplication<A, P>,
        export_bytes: usize,
    ) -> Result<Self, (ApplicationError, RoutedApplication<A, P>)> {
        let scope = match &routed.grant().input().execution {
            ExecutionMode::Single(g) if *g == routed.local() => Some(routed.grant().input().scope),
            ExecutionMode::Partitioned(v) | ExecutionMode::Delegated(v) => {
                let mut local = v
                    .iter()
                    .filter(|r| r.target == RouteTarget::Group(routed.local()));
                let scope = local.next().map(|r| r.scope);
                if local.next().is_some() {
                    None
                } else {
                    scope
                }
            }
            _ => None,
        };
        let snapshot = routed
            .readiness_requirements()
            .snapshot_bytes
            .checked_add(30 + 100 * routed.scoped_fence_limit())
            .and_then(|n| n.checked_add(export_bytes));
        if routed.applied_index() != 0
            || routed.is_initialized()
            || routed.scoped_fence_limit() == 0
            || export_bytes == 0
            || export_bytes > MAX_SCOPE_IMAGE_BYTES
            || scope != Some(routed.application().scope())
            || routed.application().scheme() != routed.grant().input().scheme
            || snapshot.is_none_or(|n| n > MAX_SCOPED_SOURCE_CHECKPOINT_BYTES)
            || routed
                .readiness_requirements()
                .command_bytes
                .checked_add(20)
                .is_none_or(|n| n > MAX_ROUTED_COMMAND_BYTES)
        {
            return Err((ApplicationError::InvalidCommand, routed));
        }
        Ok(Self {
            active_grant: routed.grant().clone(),
            routed,
            export_bytes,
            images: BTreeMap::new(),
            bound_intents: false,
            retained_grants: false,
            adoptions: Vec::new(),
            parent_limit: 0,
            parent_slots: false,
            metadata_adoption: false,
            metadata_locator_adoption: false,
        })
    }
    /// Bind each scoped freeze to its exact retained-insertion intent before bootstrap.
    /// Schema1/raw fences remain a separate profile; no live upgrade is supported.
    #[allow(clippy::result_large_err)]
    pub fn with_retained_insertion(mut self) -> Result<Self, (ApplicationError, Self)> {
        let extra = (36 + MAX_TRANSFER_INTENT_BYTES) * self.routed.scoped_fence_limit();
        if self.applied_index() != 0
            || self.routed.is_initialized()
            || self.bound_intents
            || self
                .readiness_requirements()
                .snapshot_bytes
                .checked_add(extra)
                .is_none_or(|n| n > MAX_SCOPED_SOURCE_CHECKPOINT_BYTES)
        {
            return Err((ApplicationError::InvalidCommand, self));
        }
        self.bound_intents = true;
        Ok(self)
    }
    /// Select schema3 before bootstrap; preserve original guard/history and adopt checked active grants.
    #[allow(clippy::result_large_err)]
    pub fn with_retained_grants(mut self) -> Result<Self, (ApplicationError, Self)> {
        if !self.bound_intents
            || self.retained_grants
            || self.applied_index() != 0
            || self
                .readiness_requirements()
                .snapshot_bytes
                .checked_add(
                    2 + (60 + MAX_RETAINED_ADOPTION_BYTES) * self.routed.scoped_fence_limit(),
                )
                .is_none_or(|n| n > MAX_SCOPED_SOURCE_CHECKPOINT_BYTES)
        {
            return Err((ApplicationError::InvalidCommand, self));
        }
        self.retained_grants = true;
        Ok(self)
    }
    pub fn grant(&self) -> &ResponsibilityManifest {
        &self.active_grant
    }
    pub fn fence(&self) -> Option<OwnershipFence> {
        self.routed.fence().map(|mut f| {
            f.epoch = self.grant_at(f.index).input().epoch;
            f
        })
    }
    pub fn check_context(&self, hint: &RouteHint, key: &[u8]) -> Result<(), RoutingError> {
        self.routed
            .check_grant_context(&self.active_grant, hint, key)
    }
    fn grant_at(&self, index: u64) -> ResponsibilityManifest {
        self.adoptions
            .iter()
            .rev()
            .find(|a| a.index() < index)
            .map_or_else(|| self.routed.grant().clone(), GrantChange::after)
    }
    fn adoption_operation(&self, operation: OperationId) -> bool {
        self.adoptions.iter().any(|a| a.operation() == operation)
    }
    fn checked_adoption(
        &self,
        operation: OperationId,
        command: &RetainedGrantAdoption,
    ) -> Result<Option<RetainedGrantStatus>, ApplicationError> {
        self.checked_adoption_at(
            operation,
            command,
            self.applied_index()
                .checked_add(1)
                .ok_or(ApplicationError::IndexGap)?,
        )
    }
    fn checked_adoption_at(
        &self,
        operation: OperationId,
        command: &RetainedGrantAdoption,
        index: u64,
    ) -> Result<Option<RetainedGrantStatus>, ApplicationError> {
        command.encode(MAX_RETAINED_ADOPTION_BYTES)?;
        if !self.retained_grants {
            return Err(ApplicationError::InvalidCommand);
        }
        if let Some(a) = self.adoptions.iter().find(|a| a.operation() == operation) {
            let a = a.retained().ok_or(ApplicationError::InvalidCommand)?;
            return if &a.command == command {
                Ok(Some(a.status))
            } else {
                Err(ApplicationError::InvalidCommand)
            };
        }
        let publication = &command.decision.publication;
        let intent = publication.intent();
        let status = self
            .status(publication.operation())
            .ok_or(ApplicationError::NotApplied)?;
        let source = publication
            .sources()
            .iter()
            .find(|s| s.fence.group == self.routed.local())
            .ok_or(ApplicationError::InvalidCommand)?;
        let expected =
            SourceFenceEvidence::from_scoped_status(source.configuration, status, intent)
                .map_err(|e| e.0)?;
        if self.routed.fence().is_some_and(|f| f.index <= index)
            || status.fence.fence.index >= index
            || intent.before() != self.grant()
            || source != &expected
            || status.intent_digest.is_none()
            || self
                .images
                .get(&publication.operation())
                .is_none_or(|e| e.intent.as_ref() != Some(intent))
            || self
                .adoptions
                .iter()
                .filter_map(GrantChange::retained)
                .count()
                >= self.routed.scoped_fence_limit()
            || self
                .adoptions
                .iter()
                .filter_map(GrantChange::retained)
                .any(|a| a.status.transfer == publication.operation())
            || self
                .routed
                .initialization()
                .is_some_and(|(id, _)| id == operation)
            || self.routed.has_data_operation(operation)
            || self.routed.application().contains_operation(operation)
            || self
                .routed
                .scoped_fences()
                .iter()
                .any(|f| f.fence.operation == operation)
            || self.reserved_creation(operation)
        {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(None)
    }
    fn checked_intent(
        &self,
        operation: OperationId,
        intent: &TransferIntent,
    ) -> Result<BucketRange, ApplicationError> {
        self.checked_intent_for(
            operation,
            intent,
            self.images
                .get(&operation)
                .and_then(|e| e.intent.as_ref())
                .map_or(self.grant(), |i| i.before()),
        )
    }
    fn checked_intent_for(
        &self,
        operation: OperationId,
        intent: &TransferIntent,
        grant: &ResponsibilityManifest,
    ) -> Result<BucketRange, ApplicationError> {
        let sources = intent.sources();
        if !intent.is_retained_insertion()
            || intent.before() != grant
            || sources.len() != 1
            || sources[0].target != RouteTarget::Group(self.routed.local())
            || !intent.permits_operation(operation)
            || self.reserved_creation(operation)
            || self.adoption_operation(operation)
            || self
                .routed
                .initialization()
                .is_some_and(|(id, _)| id == operation)
            || self.routed.application().contains_operation(operation)
            || self.routed.has_data_operation(operation)
            || intent.insertion_children().is_none_or(|children| {
                children.iter().any(|c| {
                    self.routed
                        .initialization()
                        .is_some_and(|(id, _)| id == c.creation)
                        || self
                            .routed
                            .fence()
                            .is_some_and(|f| f.operation == c.creation)
                        || self.routed.application().contains_operation(c.creation)
                        || self.routed.has_data_operation(c.creation)
                        || self.adoption_operation(c.creation)
                        || self
                            .routed
                            .scoped_fences()
                            .iter()
                            .any(|f| f.fence.operation == c.creation)
                })
            })
        {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(sources[0].scope)
    }
    fn reserved_creation(&self, operation: OperationId) -> bool {
        self.images.values().any(|e| {
            e.intent.as_ref().is_some_and(|i| {
                i.insertion_children()
                    .is_some_and(|children| children.iter().any(|c| c.creation == operation))
            })
        })
    }
    /// Original bootstrap/history guard. Use `grant` and `check_context` for active authority.
    pub fn routed(&self) -> &RoutedApplication<A, P> {
        &self.routed
    }
    pub fn remaining_export_bytes(&self) -> usize {
        self.export_bytes - self.images.values().map(|e| e.reservation).sum::<usize>()
    }
    /// Preflight only; it changes no authority and reserves no lifecycle phase.
    pub fn export_capacity(&self, scope: BucketRange) -> Result<usize, ApplicationError> {
        let bound = self.routed.application().export_scope_bound(scope)?;
        if bound == 0 || bound > self.remaining_export_bytes() {
            return Err(ApplicationError::ReceiptBudget);
        }
        Ok(bound)
    }
    pub fn bootstrap_command(&self, max_bytes: usize) -> Result<Vec<u8>, ApplicationError> {
        let inner = self.routed.bootstrap_command(max_bytes)?;
        if inner
            .len()
            .checked_add(if self.parent_limit == 0 { 20 } else { 22 })
            .is_none_or(|n| n > max_bytes || n > MAX_ROUTED_COMMAND_BYTES)
        {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut bytes =
            Vec::with_capacity(if self.parent_limit == 0 { 20 } else { 22 } + inner.len());
        bytes.extend(if self.metadata_locator_adoption {
            b"VBSCOWN7"
        } else if self.metadata_adoption {
            b"VBSCOWN6"
        } else if self.parent_slots {
            b"VBSCOWN5"
        } else if self.parent_limit != 0 {
            b"VBSCOWN4"
        } else if self.retained_grants {
            b"VBSCOWN3"
        } else if self.bound_intents {
            b"VBSCOWN2"
        } else {
            b"VBSCOWN1"
        });
        bytes.extend((self.export_bytes as u64).to_le_bytes());
        bytes.extend((inner.len() as u32).to_le_bytes());
        bytes.extend(inner);
        if self.parent_limit != 0 {
            bytes.extend((self.parent_limit as u16).to_le_bytes());
        }
        Ok(bytes)
    }
    fn projected(&self, entry: &LogEntry) -> Result<LogEntry, ApplicationError> {
        if matches!(&entry.payload,EntryPayload::Command{bytes,..} if bytes.len()>MAX_ROUTED_COMMAND_BYTES)
        {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut projected = entry.clone();
        if let EntryPayload::Command { operation, bytes } = &entry.payload {
            if bytes.len() > MAX_ROUTED_COMMAND_BYTES {
                return Err(ApplicationError::InvalidCommand);
            }
            let replacement = if bytes.starts_with(b"VBSCOWN1")
                || bytes.starts_with(b"VBSCOWN2")
                || bytes.starts_with(b"VBSCOWN3")
                || bytes.starts_with(b"VBSCOWN4")
                || bytes.starts_with(b"VBSCOWN5")
                || bytes.starts_with(b"VBSCOWN6")
                || bytes.starts_with(b"VBSCOWN7")
            {
                if bytes != &self.bootstrap_command(bytes.len())? {
                    return Err(ApplicationError::InvalidCommand);
                }
                self.routed.bootstrap_command(bytes.len())?
            } else if self.bound_intents && bytes.starts_with(b"VBTINT06") {
                let intent = TransferIntent::decode(bytes)?;
                if bytes != &intent.encode(bytes.len())? {
                    return Err(ApplicationError::InvalidCommand);
                }
                if self
                    .images
                    .get(operation)
                    .is_some_and(|e| e.intent.as_ref() != Some(&intent))
                {
                    return Err(ApplicationError::InvalidCommand);
                }
                let scope = self.checked_intent(*operation, &intent)?;
                if !self.images.contains_key(operation) {
                    self.export_capacity(scope)?;
                }
                encode_scope_fence(self.routed.grant().input().epoch, scope)
            } else {
                match decode(bytes, self.routed.limits().payload_bytes)? {
                    Command::Data { hint, key, payload } => {
                        if self.adoption_operation(*operation) {
                            return Err(ApplicationError::InvalidCommand);
                        }
                        if self.bound_intents
                            && self.images.iter().any(|(id, e)| {
                                *id == *operation
                                    || e.intent.as_ref().is_some_and(|i| {
                                        i.insertion_children().is_some_and(|children| {
                                            children.iter().any(|c| c.creation == *operation)
                                        })
                                    })
                            })
                        {
                            return Err(ApplicationError::InvalidCommand);
                        }
                        if self.routed.application().command_key(payload)? != key {
                            return Err(ApplicationError::InvalidCommand);
                        }
                        if self.retained_grants {
                            self.check_context(&hint, key)
                                .map_err(|_| ApplicationError::InvalidCommand)?;
                            encode_routed(
                                self.routed
                                    .original_hint(hint)
                                    .map_err(|_| ApplicationError::InvalidCommand)?,
                                key,
                                payload,
                                MAX_ROUTED_COMMAND_BYTES,
                            )?
                        } else {
                            bytes.clone()
                        }
                    }
                    Command::ScopeFence(_, scope) => {
                        if self.bound_intents {
                            return Err(ApplicationError::InvalidCommand);
                        }
                        if !self.images.contains_key(operation) {
                            if self.routed.application().contains_operation(*operation) {
                                return Err(ApplicationError::InvalidCommand);
                            }
                            self.export_capacity(scope)?;
                        }
                        bytes.clone()
                    }
                    Command::Fence(epoch) => {
                        if self.routed.application().contains_operation(*operation)
                            || (self.bound_intents && self.reserved_creation(*operation))
                            || self.adoption_operation(*operation)
                        {
                            return Err(ApplicationError::InvalidCommand);
                        }
                        if self.retained_grants {
                            if epoch != self.grant().input().epoch {
                                return Err(ApplicationError::InvalidCommand);
                            }
                            encode_fence(self.routed.grant().input().epoch)
                        } else {
                            bytes.clone()
                        }
                    }
                    Command::Bootstrap(_) | Command::ParentAdopt(_) => {
                        return Err(ApplicationError::InvalidCommand)
                    }
                }
            };
            if let EntryPayload::Command { bytes, .. } = &mut projected.payload {
                *bytes = replacement;
            }
        }
        Ok(projected)
    }
    pub fn export(
        &self,
        operation: OperationId,
        max_bytes: usize,
    ) -> Result<ScopeImage, ApplicationError> {
        let image = &self
            .images
            .get(&operation)
            .ok_or(ApplicationError::NotApplied)?
            .image;
        if image.payload_capacity() > max_bytes {
            return Err(ApplicationError::ReceiptBudget);
        }
        Ok(image.clone())
    }
    fn status(&self, operation: OperationId) -> Option<ScopedExportStatus> {
        let frozen = self.images.get(&operation)?;
        let image = &frozen.image;
        let mut fence = *self
            .routed
            .scoped_fences()
            .iter()
            .find(|f| f.fence.operation == operation)
            .expect("image has original fence");
        if let Some(intent) = &frozen.intent {
            fence.fence.epoch = intent.before().input().epoch;
        }
        Some(ScopedExportStatus {
            fence,
            digest: frozen.digest,
            schema: image.schema(),
            payload_bytes: image.bytes().len(),
            intent_digest: frozen.intent_digest,
        })
    }
    fn checked_image(
        &self,
        image: &ScopeImage,
        fence: ScopedOwnershipFence,
    ) -> Result<(), ApplicationError> {
        if image.schema() != self.routed.application().schema_version()
            || image.scheme() != self.routed.grant().input().scheme
            || image.scope() != fence.scope
            || image.source_applied() != fence.fence.index
            || image.payload_capacity()
                > self.routed.application().export_scope_bound(fence.scope)?
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        Ok(())
    }
    pub fn readiness_requirements(&self) -> crate::raft::ReadinessRequirements {
        let inner = self.routed.readiness_requirements();
        crate::raft::ReadinessRequirements {
            application_schema: if self.metadata_locator_adoption {
                LOCATOR_SCOPED_TRANSFER_SOURCE_SCHEMA
            } else if self.metadata_adoption {
                METADATA_SCOPED_TRANSFER_SOURCE_SCHEMA
            } else if self.parent_slots {
                PARENT_SLOT_SCOPED_TRANSFER_SOURCE_SCHEMA
            } else if self.parent_limit != 0 {
                PARENT_SCOPED_TRANSFER_SOURCE_SCHEMA
            } else if self.retained_grants {
                RETAINED_SCOPED_TRANSFER_SOURCE_SCHEMA
            } else if self.bound_intents {
                BOUND_SCOPED_TRANSFER_SOURCE_SCHEMA
            } else {
                SCOPED_TRANSFER_SOURCE_SCHEMA
            },
            command_bytes: (inner.command_bytes + if self.parent_limit == 0 { 20 } else { 22 })
                .max(if self.metadata_adoption {
                    MAX_RETAINED_ADOPTION_BYTES.max(MAX_METADATA_ADOPTION_BYTES)
                } else if self.parent_slots {
                    MAX_RETAINED_ADOPTION_BYTES.max(MAX_PARENT_SLOT_ADOPTION_BYTES)
                } else if self.parent_limit != 0 {
                    MAX_RETAINED_ADOPTION_BYTES.max(MAX_CROSS_PARENT_ADOPTION_BYTES)
                } else if self.retained_grants {
                    MAX_RETAINED_ADOPTION_BYTES
                } else if self.bound_intents {
                    MAX_TRANSFER_INTENT_BYTES
                } else {
                    0
                }),
            snapshot_bytes: inner.snapshot_bytes
                + 30
                + 100 * self.routed.scoped_fence_limit()
                + self.export_bytes
                + if self.bound_intents {
                    (36 + MAX_TRANSFER_INTENT_BYTES) * self.routed.scoped_fence_limit()
                } else {
                    0
                }
                + if self.retained_grants {
                    2 + (60 + MAX_RETAINED_ADOPTION_BYTES) * self.routed.scoped_fence_limit()
                } else {
                    0
                }
                + if self.parent_limit == 0 {
                    0
                } else {
                    2 + self.routed.scoped_fence_limit()
                        + self.parent_limit * (61 + self.parent_command_bound())
                },
        }
    }
}
impl<A, P> StateMachine for ScopedTransferSource<A, P>
where
    A: ScopeStateMachine + BoundedStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    type Receipt = RoutedReceipt<A::Receipt>;
    fn validate_group(&self, g: GroupIdentity) -> Result<(), ApplicationError> {
        self.routed.validate_group(g)
    }
    fn deployment_requirements(&self) -> Option<crate::raft::ReadinessRequirements> {
        Some(self.readiness_requirements())
    }
    fn applied_index(&self) -> u64 {
        self.routed.applied_index()
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
            if let EntryPayload::Command { operation, bytes } = &entry.payload {
                if parent_command(bytes) {
                    let status = next.apply_parent(*operation, entry.index, bytes)?;
                    next.routed.apply_batch(&[LogEntry {
                        index: entry.index,
                        term: entry.term,
                        payload: EntryPayload::Noop,
                    }])?;
                    receipts.push(RoutedReceipt {
                        index: entry.index,
                        operation: *operation,
                        outcome: RoutedOutcome::ParentAdopted(status),
                    });
                    continue;
                }
                if bytes.starts_with(b"VBSADP01") {
                    let command = RetainedGrantAdoption::decode(bytes)?;
                    let original = next.checked_adoption(*operation, &command)?;
                    let noop = LogEntry {
                        index: entry.index,
                        term: entry.term,
                        payload: EntryPayload::Noop,
                    };
                    next.routed.apply_batch(&[noop])?;
                    let status = original.unwrap_or(RetainedGrantStatus {
                        operation: *operation,
                        index: entry.index,
                        transfer: command.decision.publication.operation(),
                        epoch: command.decision.publication.intent().after().input().epoch,
                        generation: command
                            .decision
                            .publication
                            .intent()
                            .after()
                            .input()
                            .generation,
                    });
                    if original.is_none() {
                        next.active_grant = command.decision.publication.intent().after().clone();
                        next.adoptions
                            .try_reserve_exact(1)
                            .map_err(|_| ApplicationError::ReceiptBudget)?;
                        next.adoptions
                            .push(GrantChange::Retained(Adoption::new(status, command)?));
                    }
                    receipts.push(RoutedReceipt {
                        index: entry.index,
                        operation: *operation,
                        outcome: RoutedOutcome::GrantAdopted(status),
                    });
                    continue;
                }
                if next.retained_grants {
                    if let Ok(Command::Data { hint, key, .. }) =
                        decode(bytes, next.routed.limits().payload_bytes)
                    {
                        if let Err(error) = next.check_context(&hint, key) {
                            next.routed.apply_batch(&[LogEntry {
                                index: entry.index,
                                term: entry.term,
                                payload: EntryPayload::Noop,
                            }])?;
                            receipts.push(RoutedReceipt {
                                index: entry.index,
                                operation: *operation,
                                outcome: RoutedOutcome::Rejected(error),
                            });
                            continue;
                        }
                    }
                }
            }
            let projected = next.projected(entry)?;
            let mut applied = next.routed.apply_batch(std::slice::from_ref(&projected))?;
            for r in &applied {
                if let RoutedOutcome::ScopeFenced(fence) = r.outcome {
                    if !next.images.contains_key(&fence.fence.operation) {
                        let bound = next.export_capacity(fence.scope)?;
                        let image = next.routed.application().export_scope(fence.scope, bound)?;
                        next.checked_image(&image, fence)?;
                        if image.payload_capacity() > next.remaining_export_bytes() {
                            return Err(ApplicationError::ReceiptBudget);
                        }
                        next.images.insert(
                            fence.fence.operation,
                            FrozenExport {
                                digest: ContentDigest::scope_image(&image),
                                image,
                                reservation: bound,
                                intent: if next.bound_intents {
                                    let EntryPayload::Command { bytes, .. } = &entry.payload else {
                                        return Err(ApplicationError::InvalidCommand);
                                    };
                                    Some(TransferIntent::decode(bytes)?)
                                } else {
                                    None
                                },
                                intent_digest: if next.bound_intents {
                                    let EntryPayload::Command { bytes, .. } = &entry.payload else {
                                        return Err(ApplicationError::InvalidCommand);
                                    };
                                    Some(ContentDigest::sha256(bytes))
                                } else {
                                    None
                                },
                            },
                        );
                    }
                }
            }
            if next.retained_grants {
                for r in &mut applied {
                    match &mut r.outcome {
                        RoutedOutcome::ScopeFenced(f) => {
                            if let Some(i) = next
                                .images
                                .get(&f.fence.operation)
                                .and_then(|e| e.intent.as_ref())
                            {
                                f.fence.epoch = i.before().input().epoch;
                            }
                        }
                        RoutedOutcome::Fenced(f) => f.epoch = next.grant_at(f.index).input().epoch,
                        _ => {}
                    }
                }
            }
            receipts.extend(applied);
        }
        *self = next;
        Ok(receipts)
    }
}
impl<A, P> BoundedStateMachine for ScopedTransferSource<A, P>
where
    A: ScopeStateMachine + BoundedStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        let mut next = self.clone();
        let receipts = next.apply_batch(entries)?;
        let mut bound = receipts
            .len()
            .checked_mul(size_of::<Self::Receipt>())
            .ok_or(ApplicationError::ReceiptBudget)?;
        for r in receipts {
            bound = bound
                .checked_add(r.nested_bytes(usize::MAX)?)
                .ok_or(ApplicationError::ReceiptBudget)?;
        }
        Ok(bound)
    }
}
impl<A, P> ProposalAdmission for ScopedTransferSource<A, P>
where
    A: ScopeStateMachine + BoundedStateMachine + ProposalAdmission,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    fn validate_proposal<'a>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError> {
        let mut next = self.clone();
        for (position, (id, bytes)) in pending.enumerate() {
            if position >= MAX_ROUTED_PENDING {
                return Err(ApplicationError::ReceiptBudget);
            }
            if bytes.len() > MAX_ROUTED_COMMAND_BYTES {
                return Err(ApplicationError::InvalidCommand);
            }
            let entry = LogEntry {
                index: next
                    .applied_index()
                    .checked_add(1)
                    .ok_or(ApplicationError::IndexGap)?,
                term: 1,
                payload: EntryPayload::Command {
                    operation: id,
                    bytes: bytes.to_vec(),
                },
            };
            next.apply_batch(&[entry])?;
        }
        if bytes.len() > MAX_ROUTED_COMMAND_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let entry = LogEntry {
            index: next
                .applied_index()
                .checked_add(1)
                .ok_or(ApplicationError::IndexGap)?,
            term: 1,
            payload: EntryPayload::Command {
                operation,
                bytes: bytes.to_vec(),
            },
        };
        if bytes.starts_with(b"VBSADP01") || parent_command(bytes) {
            let bound = next.receipt_bytes_bound(std::slice::from_ref(&entry))?;
            next.apply_batch(&[entry])?;
            return Ok(bound);
        }
        let projected = next.projected(&entry)?;
        let EntryPayload::Command { bytes, .. } = &projected.payload else {
            return Err(ApplicationError::InvalidCommand);
        };
        next.routed
            .validate_proposal(operation, bytes, std::iter::empty())?;
        let bound = next.receipt_bytes_bound(std::slice::from_ref(&entry))?;
        next.apply_batch(&[entry])?;
        Ok(bound)
    }
}
impl<A, P> CheckpointStateMachine for ScopedTransferSource<A, P>
where
    A: ScopeStateMachine + BoundedStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    fn schema_version(&self) -> u64 {
        self.readiness_requirements().application_schema
    }
    fn checkpoint(&self, max_bytes: usize) -> Result<Vec<u8>, ApplicationError> {
        let inner = self.routed.checkpoint(max_bytes)?;
        let len = 30
            + inner.len()
            + self
                .images
                .values()
                .map(|e| {
                    100 + e.image.bytes().len()
                        + e.intent.as_ref().map_or(0, |i| {
                            36 + i
                                .encode(MAX_TRANSFER_INTENT_BYTES)
                                .expect("checked intent")
                                .len()
                        })
                })
                .sum::<usize>()
            + if self.retained_grants {
                2 + self
                    .adoptions
                    .iter()
                    .map(|a| {
                        60 + usize::from(self.parent_limit != 0)
                            + a.command().expect("checked adoption").len()
                    })
                    .sum::<usize>()
            } else {
                0
            };
        let len = len + if self.parent_limit == 0 { 0 } else { 2 };
        if len > max_bytes || len > self.readiness_requirements().snapshot_bytes {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut out = Vec::with_capacity(len);
        out.extend(if self.metadata_locator_adoption {
            b"VBSCCHK7"
        } else if self.metadata_adoption {
            b"VBSCCHK6"
        } else if self.parent_slots {
            b"VBSCCHK5"
        } else if self.parent_limit != 0 {
            b"VBSCCHK4"
        } else if self.retained_grants {
            b"VBSCCHK3"
        } else if self.bound_intents {
            b"VBSCCHK2"
        } else {
            b"VBSCCHK1"
        });
        out.extend(self.applied_index().to_le_bytes());
        out.extend((self.export_bytes as u64).to_le_bytes());
        out.extend((inner.len() as u32).to_le_bytes());
        out.extend(inner);
        if self.parent_limit != 0 {
            out.extend((self.parent_limit as u16).to_le_bytes());
        }
        out.extend((self.images.len() as u16).to_le_bytes());
        for (op, e) in &self.images {
            let i = &e.image;
            out.extend((e.reservation as u64).to_le_bytes());
            out.extend(op.get().to_le_bytes());
            out.extend(e.digest.0);
            out.extend(i.schema().to_le_bytes());
            out.extend(i.scheme().id.get().to_le_bytes());
            out.extend(i.scheme().version.to_le_bytes());
            out.extend(i.scope().start().to_le_bytes());
            out.extend(i.scope().end().to_le_bytes());
            out.extend(i.source_applied().to_le_bytes());
            out.extend((i.bytes().len() as u32).to_le_bytes());
            out.extend(i.bytes());
            if self.bound_intents {
                out.extend(
                    e.intent_digest
                        .ok_or(ApplicationError::InvalidCheckpoint)?
                        .0,
                );
                let intent = e
                    .intent
                    .as_ref()
                    .ok_or(ApplicationError::InvalidCheckpoint)?
                    .encode(MAX_TRANSFER_INTENT_BYTES)?;
                out.extend((intent.len() as u32).to_le_bytes());
                out.extend(intent);
            }
        }
        if self.retained_grants {
            out.extend((self.adoptions.len() as u16).to_le_bytes());
            for a in &self.adoptions {
                let command = a.command()?;
                if self.parent_limit != 0 {
                    out.push(u8::from(matches!(a, GrantChange::Parent { .. })));
                }
                out.extend(a.digest()?.0);
                out.extend(a.operation().get().to_le_bytes());
                out.extend(a.index().to_le_bytes());
                out.extend((command.len() as u32).to_le_bytes());
                out.extend(command);
            }
        }
        Ok(out)
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
            let mut r = Reader::new(bytes);
            if r.take(8)?
                != if self.metadata_locator_adoption {
                    b"VBSCCHK7"
                } else if self.metadata_adoption {
                    b"VBSCCHK6"
                } else if self.parent_slots {
                    b"VBSCCHK5"
                } else if self.parent_limit != 0 {
                    b"VBSCCHK4"
                } else if self.retained_grants {
                    b"VBSCCHK3"
                } else if self.bound_intents {
                    b"VBSCCHK2"
                } else {
                    b"VBSCCHK1"
                }
                || r.u64()? != applied
                || r.u64()? != self.export_bytes as u64
            {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            let len = r.u32()? as usize;
            let mut next = self.clone();
            next.active_grant = next.routed.grant().clone();
            next.adoptions.clear();
            next.routed.restore_checkpoint(
                SCOPED_ROUTED_APPLICATION_SCHEMA,
                applied,
                r.take(len)?,
            )?;
            if self.parent_limit != 0 && usize::from(r.u16()?) != self.parent_limit {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            let count = usize::from(r.u16()?);
            if count != next.routed.scoped_fences().len() {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            next.images.clear();
            let mut previous = 0;
            let mut retained = 0usize;
            for _ in 0..count {
                let reservation =
                    usize::try_from(r.u64()?).map_err(|_| ApplicationError::InvalidCheckpoint)?;
                let op = r.operation()?;
                if op.get() <= previous {
                    return Err(ApplicationError::InvalidCheckpoint);
                }
                previous = op.get();
                let digest = ContentDigest(
                    r.take(32)?
                        .try_into()
                        .map_err(|_| ApplicationError::InvalidCheckpoint)?,
                );
                let schema = r.u64()?;
                let scheme = PartitionScheme {
                    id: RoutingSchemeId::new(r.u128()?)
                        .ok_or(ApplicationError::InvalidCheckpoint)?,
                    version: r.u32()?,
                };
                let scope = r.range()?;
                let index = r.u64()?;
                let len = r.u32()? as usize;
                if reservation == 0
                    || reservation > next.export_bytes - retained
                    || len == 0
                    || len > reservation
                {
                    return Err(ApplicationError::InvalidCheckpoint);
                }
                let image = ScopeImage::new(schema, scheme, scope, index, r.take(len)?.to_vec())
                    .map_err(|e| e.0)?;
                let fence = *next
                    .routed
                    .scoped_fences()
                    .iter()
                    .find(|s| s.fence.operation == op)
                    .ok_or(ApplicationError::InvalidCheckpoint)?;
                next.checked_image(&image, fence)?;
                if reservation != next.routed.application().export_scope_bound(fence.scope)?
                    || image.payload_capacity() > reservation
                {
                    return Err(ApplicationError::InvalidCheckpoint);
                }
                if digest != ContentDigest::scope_image(&image) {
                    return Err(ApplicationError::InvalidCheckpoint);
                }
                let intent = if next.bound_intents {
                    let original_digest = ContentDigest(
                        r.take(32)?
                            .try_into()
                            .map_err(|_| ApplicationError::InvalidCheckpoint)?,
                    );
                    let len = r.u32()? as usize;
                    let intent = TransferIntent::decode(r.take(len)?)?;
                    if next.checked_intent_for(op, &intent, intent.before())? != fence.scope {
                        return Err(ApplicationError::InvalidCheckpoint);
                    }
                    if original_digest
                        != ContentDigest::sha256(&intent.encode(MAX_TRANSFER_INTENT_BYTES)?)
                    {
                        return Err(ApplicationError::InvalidCheckpoint);
                    }
                    Some(intent)
                } else {
                    None
                };
                let intent_digest = intent
                    .as_ref()
                    .map(|i| {
                        i.encode(MAX_TRANSFER_INTENT_BYTES)
                            .map(|b| ContentDigest::sha256(&b))
                    })
                    .transpose()?;
                retained = retained
                    .checked_add(reservation)
                    .filter(|n| *n <= next.export_bytes)
                    .ok_or(ApplicationError::InvalidCheckpoint)?;
                next.images.insert(
                    op,
                    FrozenExport {
                        digest,
                        image,
                        reservation,
                        intent,
                        intent_digest,
                    },
                );
            }
            if next.retained_grants {
                let count = usize::from(r.u16()?);
                if count > next.routed.scoped_fence_limit() + next.parent_limit {
                    return Err(ApplicationError::InvalidCheckpoint);
                }
                let mut last = 0;
                for _ in 0..count {
                    let parent = self.parent_limit != 0 && r.boolean()?;
                    let digest = ContentDigest(
                        r.take(32)?
                            .try_into()
                            .map_err(|_| ApplicationError::InvalidCheckpoint)?,
                    );
                    let operation = r.operation()?;
                    let index = r.u64()?;
                    let len = r.u32()? as usize;
                    let command_bytes = r.take(len)?;
                    if index == 0
                        || index <= last
                        || index > applied
                        || next.routed.has_semantic_index(index)
                        || next.adoption_operation(operation)
                    {
                        return Err(ApplicationError::InvalidCheckpoint);
                    }
                    let change = if parent {
                        next.checked_parent(
                            operation,
                            index,
                            crate::routed::parent_adoption::ParentAdoptionCommand::decode_scoped_metadata(
                                command_bytes,
                                next.parent_slots,
                                next.metadata_adoption, next.metadata_locator_adoption
                            )?,
                        )?
                    } else {
                        let command = RetainedGrantAdoption::decode(command_bytes)?;
                        if next
                            .checked_adoption_at(operation, &command, index)?
                            .is_some()
                        {
                            return Err(ApplicationError::InvalidCheckpoint);
                        }
                        let intent = command.decision.publication.intent();
                        let status = RetainedGrantStatus {
                            operation,
                            index,
                            transfer: command.decision.publication.operation(),
                            epoch: intent.after().input().epoch,
                            generation: intent.after().input().generation,
                        };
                        GrantChange::Retained(Adoption::new(status, command)?)
                    };
                    if change.digest()? != digest {
                        return Err(ApplicationError::InvalidCheckpoint);
                    }
                    next.active_grant = change.after();
                    next.adoptions
                        .try_reserve_exact(1)
                        .map_err(|_| ApplicationError::ReceiptBudget)?;
                    next.adoptions.push(change);
                    last = index;
                }
                for (operation, e) in &next.images {
                    let intent = e
                        .intent
                        .as_ref()
                        .ok_or(ApplicationError::InvalidCheckpoint)?;
                    if intent.before() != &next.grant_at(e.image.source_applied())
                        || next
                            .checked_intent_for(*operation, intent, intent.before())
                            .is_err()
                    {
                        return Err(ApplicationError::InvalidCheckpoint);
                    }
                }
            }
            if !r.done() {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            Ok(next)
        };
        *self = restore().map_err(|_| ApplicationError::InvalidCheckpoint)?;
        Ok(())
    }
}
impl<A, P> ReadableStateMachine for ScopedTransferSource<A, P>
where
    A: ScopeStateMachine + BoundedStateMachine + ReadableStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    type Query = ScopedSourceQuery<A::Query>;
    type ReadResult = ScopedSourceRead<A::ReadResult>;
    fn read_at(
        &self,
        index: u64,
        query: Self::Query,
    ) -> Result<Self::ReadResult, ApplicationError> {
        if index > self.applied_index() {
            return Err(ApplicationError::NotApplied);
        }
        match query {
            ScopedSourceQuery::Data(mut q) => {
                if self.retained_grants {
                    if let Err(error) = self.check_context(&q.hint, &q.key) {
                        return Ok(ScopedSourceRead::Data(RoutedRead::Rejected(error)));
                    }
                    q.hint = self
                        .routed
                        .original_hint(q.hint)
                        .map_err(|_| ApplicationError::InvalidCommand)?;
                }
                self.routed.read_at(index, q).map(ScopedSourceRead::Data)
            }
            ScopedSourceQuery::Frozen(op) => Ok(ScopedSourceRead::Frozen(self.status(op))),
            ScopedSourceQuery::ParentAdoption(op) => {
                if self.parent_limit == 0 {
                    return Err(ApplicationError::UnsupportedSchema);
                }
                Ok(ScopedSourceRead::ParentAdoption(self.parent_adoption(op)))
            }
            ScopedSourceQuery::MetadataLocatorAdoption(op) => {
                if !self.metadata_locator_adoption {
                    return Err(ApplicationError::UnsupportedSchema);
                }
                Ok(ScopedSourceRead::MetadataLocatorAdoption(
                    self.metadata_locator_adoption(op),
                ))
            }
            ScopedSourceQuery::MetadataAdoption(op) => {
                if !self.metadata_adoption {
                    return Err(ApplicationError::UnsupportedSchema);
                }
                Ok(ScopedSourceRead::MetadataAdoption(
                    self.metadata_adoption(op),
                ))
            }
            ScopedSourceQuery::Grant(op) => {
                if self.retained_grants {
                    Ok(ScopedSourceRead::Grant(
                        self.adoptions
                            .iter()
                            .filter_map(GrantChange::retained)
                            .find(|a| a.status.operation == op)
                            .map(|a| a.status),
                    ))
                } else {
                    Err(ApplicationError::UnsupportedSchema)
                }
            }
        }
    }
}
impl<A, P> BoundedReadableStateMachine for ScopedTransferSource<A, P>
where
    A: ScopeStateMachine + BoundedStateMachine + BoundedReadableStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    fn query_bytes(&self, q: &Self::Query, limit: usize) -> Result<usize, ApplicationError> {
        match q {
            ScopedSourceQuery::Data(q) => self.routed.query_bytes(q, limit),
            ScopedSourceQuery::Frozen(_)
            | ScopedSourceQuery::Grant(_)
            | ScopedSourceQuery::ParentAdoption(_)
            | ScopedSourceQuery::MetadataLocatorAdoption(_)
            | ScopedSourceQuery::MetadataAdoption(_) => Ok(0),
        }
    }
    fn read_result_bound(&self, q: &Self::Query) -> Result<usize, ApplicationError> {
        let nested = match q {
            ScopedSourceQuery::Data(q) => self
                .routed
                .read_result_bound(q)?
                .checked_sub(size_of::<RoutedRead<A::ReadResult>>())
                .ok_or(ApplicationError::ReceiptBudget)?,
            ScopedSourceQuery::Frozen(_)
            | ScopedSourceQuery::Grant(_)
            | ScopedSourceQuery::ParentAdoption(_)
            | ScopedSourceQuery::MetadataLocatorAdoption(_)
            | ScopedSourceQuery::MetadataAdoption(_) => 0,
        };
        size_of::<Self::ReadResult>()
            .checked_add(nested)
            .ok_or(ApplicationError::ReceiptBudget)
    }
    fn read_result_bytes(
        &self,
        r: &Self::ReadResult,
        limit: usize,
    ) -> Result<usize, ApplicationError> {
        match r {
            ScopedSourceRead::Data(r) => self.routed.read_result_bytes(r, limit),
            ScopedSourceRead::Frozen(_)
            | ScopedSourceRead::Grant(_)
            | ScopedSourceRead::ParentAdoption(_)
            | ScopedSourceRead::MetadataLocatorAdoption(_)
            | ScopedSourceRead::MetadataAdoption(_) => Ok(0),
        }
    }
}
