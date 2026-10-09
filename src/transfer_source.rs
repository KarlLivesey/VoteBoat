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
//! Forward-only source fence with exact-boundary scope export.
use crate::{
    application::*,
    identity::*,
    log::*,
    routed::codec::{decode, Command},
    routed::*,
    routing::codec::Reader,
    routing::*,
    scope::*,
    transfer::*,
};
use std::mem::size_of;

pub const TRANSFER_SOURCE_SCHEMA: u64 = 1;

/// Local applied status; only an authenticated quorum-backed observation can
/// establish it for another group. This is not a cryptographic certificate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceFreezeStatus {
    pub fence: OwnershipFence,
    pub intent: TransferIntent,
    pub exports: Vec<SourceExportCommitment>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourceExportCommitment {
    pub target: GroupIdentity,
    pub scope: BucketRange,
    pub digest: ContentDigest,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SourceQuery<Q> {
    Data(RoutedQuery<Q>),
    Freeze,
}
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(clippy::large_enum_variant)] // Inline layout is charged by read_result_bound.
pub enum SourceRead<R> {
    Data(RoutedRead<R>),
    Freeze(Option<SourceFreezeStatus>),
}

/// Source-side guard over the same routed application and storage binding.
/// After freeze, `applied` progresses while routed data remains exactly at F.
#[derive(Clone)]
pub struct TransferSource<A, P> {
    routed: RoutedApplication<A, P>,
    export_bytes: usize,
    applied: u64,
    intent: Option<TransferIntent>,
}
impl<A, P> TransferSource<A, P>
where
    A: ScopeStateMachine + BoundedStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    #[allow(clippy::result_large_err)] // Construction rejection returns the original application.
    pub fn new(
        routed: RoutedApplication<A, P>,
        export_bytes: usize,
    ) -> Result<Self, (ApplicationError, RoutedApplication<A, P>)> {
        let scope = Self::local_scope(&routed);
        if routed.applied_index() != 0
            || routed.scoped_fence_limit() != 0
            || routed.is_initialized()
            || routed.fence().is_some()
            || export_bytes == 0
            || export_bytes > MAX_SCOPE_IMAGE_BYTES
            || scope != Some(routed.application().scope())
            || routed.application().scheme() != routed.grant().input().scheme
        {
            return Err((ApplicationError::InvalidCommand, routed));
        }
        Ok(Self {
            routed,
            export_bytes,
            applied: 0,
            intent: None,
        })
    }
    fn local_scope(routed: &RoutedApplication<A, P>) -> Option<BucketRange> {
        let input = routed.grant().input();
        match &input.execution {
            ExecutionMode::Single(g) if *g == routed.local() => Some(input.scope),
            ExecutionMode::Partitioned(routes) => {
                let mut local = routes
                    .iter()
                    .filter(|r| r.target == RouteTarget::Group(routed.local()));
                let scope = local.next()?.scope;
                local.next().is_none().then_some(scope)
            }
            _ => None,
        }
    }
    pub fn routed(&self) -> &RoutedApplication<A, P> {
        &self.routed
    }
    pub fn frozen_intent(&self) -> Option<&TransferIntent> {
        self.intent.as_ref()
    }
    pub fn fence(&self) -> Option<OwnershipFence> {
        self.routed.fence()
    }
    pub fn bootstrap_command(&self, max_bytes: usize) -> Result<Vec<u8>, ApplicationError> {
        let inner = self.routed.bootstrap_command(max_bytes)?;
        if inner.len().checked_add(20).is_none_or(|n| n > max_bytes) {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut bytes = Vec::with_capacity(20 + inner.len());
        bytes.extend(b"VBSROWN1");
        bytes.extend((self.export_bytes as u64).to_le_bytes());
        bytes.extend((inner.len() as u32).to_le_bytes());
        bytes.extend(inner);
        Ok(bytes)
    }
    /// Encode only; a trusted authenticated host must verify the committed
    /// directory intent and target staging/capacity before proposing a freeze.
    pub fn freeze_command(
        intent: &TransferIntent,
        max_bytes: usize,
    ) -> Result<Vec<u8>, ApplicationError> {
        let inner = intent.encode(max_bytes)?;
        if inner.len().checked_add(8).is_none_or(|n| n > max_bytes) {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut bytes = Vec::with_capacity(8 + inner.len());
        bytes.extend(b"VBSFREE1");
        bytes.extend(inner);
        Ok(bytes)
    }
    fn checked_intent(&self, bytes: &[u8]) -> Result<TransferIntent, ApplicationError> {
        let intent = TransferIntent::decode(bytes)?;
        if intent.before() != self.routed.grant() {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut total = 0usize;
        for (_, scope) in self.export_ranges(&intent)? {
            let bound = self.routed.application().export_scope_bound(scope)?;
            if bound == 0 || bound > MAX_SCOPE_IMAGE_BYTES {
                return Err(ApplicationError::InvalidCommand);
            }
            total = total
                .checked_add(bound)
                .ok_or(ApplicationError::InvalidCommand)?;
        }
        if total > self.export_bytes {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(intent)
    }
    fn export_ranges(
        &self,
        intent: &TransferIntent,
    ) -> Result<Vec<(GroupIdentity, BucketRange)>, ApplicationError> {
        let scope = self.routed.application().scope();
        let mut result = Vec::new();
        let mut cursor = scope.start();
        for target in intent.targets() {
            let start = scope.start().max(target.scope.start());
            let end = scope.end().min(target.scope.end());
            if start >= end {
                continue;
            }
            if cursor != start {
                return Err(ApplicationError::InvalidCommand);
            }
            let RouteTarget::Group(group) = target.target else {
                return Err(ApplicationError::InvalidCommand);
            };
            result.push((
                group,
                BucketRange::new(start, end).map_err(|_| ApplicationError::InvalidCommand)?,
            ));
            cursor = end;
        }
        if cursor != scope.end() {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(result)
    }
    /// Immutable local export, not a transferable quorum/fence certificate.
    pub fn export_target(
        &self,
        target: GroupIdentity,
        max_bytes: usize,
    ) -> Result<ScopeImage, ApplicationError> {
        let intent = self.intent.as_ref().ok_or(ApplicationError::NotApplied)?;
        let scope = self
            .export_ranges(intent)?
            .into_iter()
            .find(|(g, _)| *g == target)
            .ok_or(ApplicationError::InvalidCommand)?
            .1;
        let bound = self.routed.application().export_scope_bound(scope)?;
        let image = self
            .routed
            .application()
            .export_scope(scope, bound.min(max_bytes))?;
        if image.scope() != scope
            || image.scheme() != self.routed.grant().input().scheme
            || image.schema() != self.routed.application().schema_version()
            || image.source_applied() != self.fence().ok_or(ApplicationError::NotApplied)?.index
            || image.payload_capacity() > bound
            || image.payload_capacity() > max_bytes
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        Ok(image)
    }
    fn projected(
        &self,
        entry: &LogEntry,
    ) -> Result<(LogEntry, Option<TransferIntent>), ApplicationError> {
        if matches!(&entry.payload, EntryPayload::Command { bytes, .. } if bytes.len() > MAX_ROUTED_COMMAND_BYTES + 20)
        {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut next = entry.clone();
        let mut freeze = None;
        if let EntryPayload::Command { operation, bytes } = &entry.payload {
            let replacement = if bytes.starts_with(b"VBSROWN1") {
                if bytes != &self.bootstrap_command(bytes.len())? {
                    return Err(ApplicationError::InvalidCommand);
                }
                self.routed.bootstrap_command(bytes.len())?
            } else if let Some(inner) = bytes.strip_prefix(b"VBSFREE1") {
                if !self.routed.is_initialized() {
                    return Err(ApplicationError::NotApplied);
                }
                let intent = self.checked_intent(inner)?;
                if !intent.permits_operation(*operation) {
                    return Err(ApplicationError::InvalidCommand);
                }
                freeze = Some(intent);
                encode_fence(self.routed.grant().input().epoch)
            } else {
                match decode(bytes, self.routed.limits().payload_bytes)? {
                    Command::Data { key, payload, .. } => {
                        if self.routed.application().command_key(payload)? != key {
                            return Err(ApplicationError::InvalidCommand);
                        }
                    }
                    _ => return Err(ApplicationError::InvalidCommand), // No unbound bootstrap or fence bypass.
                }
                bytes.clone()
            };
            if let EntryPayload::Command { bytes, .. } = &mut next.payload {
                *bytes = replacement;
            }
        }
        Ok((next, freeze))
    }
    pub fn readiness_requirements(&self) -> crate::raft::ReadinessRequirements {
        let inner = self.routed.readiness_requirements();
        crate::raft::ReadinessRequirements {
            application_schema: TRANSFER_SOURCE_SCHEMA,
            command_bytes: (inner.command_bytes + 20).max(8 + MAX_TRANSFER_INTENT_BYTES),
            snapshot_bytes: inner.snapshot_bytes + 40 + MAX_TRANSFER_INTENT_BYTES,
        }
    }
}
impl<A, P> StateMachine for TransferSource<A, P>
where
    A: ScopeStateMachine + BoundedStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    type Receipt = RoutedReceipt<A::Receipt>;
    fn validate_group(&self, group: GroupIdentity) -> Result<(), ApplicationError> {
        self.routed.validate_group(group)
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
            let (projected, intent) = next.projected(entry)?;
            if let Some(fence) = next.fence() {
                if let EntryPayload::Command { operation, .. } = entry.payload {
                    let outcome = if operation == fence.operation
                        && intent.as_ref() == next.intent.as_ref()
                    {
                        RoutedOutcome::Fenced(fence)
                    } else {
                        RoutedOutcome::Rejected(RoutingError::Fenced)
                    };
                    receipts.push(RoutedReceipt {
                        index: entry.index,
                        operation,
                        outcome,
                    });
                }
            } else {
                let applied = next.routed.apply_batch(std::slice::from_ref(&projected))?;
                if next.fence().is_some() {
                    next.intent = intent;
                    // Check actual provider output before publishing the candidate state.
                    for (target, _) in next.export_ranges(
                        next.intent
                            .as_ref()
                            .ok_or(ApplicationError::InvalidCommand)?,
                    )? {
                        next.export_target(target, next.export_bytes)?;
                    }
                }
                receipts.extend(applied);
            }
            next.applied = entry.index;
        }
        *self = next;
        Ok(receipts)
    }
}
impl<A, P> BoundedStateMachine for TransferSource<A, P>
where
    A: ScopeStateMachine + BoundedStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        // Simulate on bounded owned state, including bootstrap/fence within this batch.
        // This avoids delegating a later command against pre-bootstrap state.
        let mut next = self.clone();
        let mut bound = 0usize;
        for entry in entries {
            let (projected, _) = next.projected(entry)?;
            let bytes = if next.fence().is_some() {
                if matches!(entry.payload, EntryPayload::Command { .. }) {
                    size_of::<Self::Receipt>()
                } else {
                    0
                }
            } else {
                next.routed
                    .receipt_bytes_bound(std::slice::from_ref(&projected))?
            };
            bound = bound
                .checked_add(bytes)
                .ok_or(ApplicationError::ReceiptBudget)?;
            next.apply_batch(std::slice::from_ref(entry))?;
        }
        Ok(bound)
    }
}
impl<A, P> ProposalAdmission for TransferSource<A, P>
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
        if bytes.len() > MAX_ROUTED_COMMAND_BYTES + 20 {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut next = self.clone();
        for (position, (id, bytes)) in pending.enumerate() {
            if position >= MAX_ROUTED_PENDING || bytes.len() > MAX_ROUTED_COMMAND_BYTES + 20 {
                return Err(ApplicationError::ReceiptBudget);
            }
            let entry = LogEntry {
                index: next
                    .applied
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
        let entry = LogEntry {
            index: next
                .applied
                .checked_add(1)
                .ok_or(ApplicationError::IndexGap)?,
            term: 1,
            payload: EntryPayload::Command {
                operation,
                bytes: bytes.to_vec(),
            },
        };
        let (projected, _) = next.projected(&entry)?;
        if next.fence().is_some() {
            if next.frozen_intent() != next.projected(&entry)?.1.as_ref()
                || next.fence().is_none_or(|f| f.operation != operation)
                || !bytes.starts_with(b"VBSFREE1")
            {
                return Err(ApplicationError::InvalidCommand);
            }
        } else if let EntryPayload::Command { bytes, .. } = &projected.payload {
            next.routed
                .validate_proposal(operation, bytes, std::iter::empty())?;
        }
        let bound = next.receipt_bytes_bound(std::slice::from_ref(&entry))?;
        next.apply_batch(&[entry])?;
        Ok(bound)
    }
}
impl<A, P> CheckpointStateMachine for TransferSource<A, P>
where
    A: ScopeStateMachine + BoundedStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    fn schema_version(&self) -> u64 {
        TRANSFER_SOURCE_SCHEMA
    }
    fn checkpoint(&self, max_bytes: usize) -> Result<Vec<u8>, ApplicationError> {
        let inner = self.routed.checkpoint(max_bytes)?;
        let intent = self
            .intent
            .as_ref()
            .map(|i| i.encode(MAX_TRANSFER_INTENT_BYTES))
            .transpose()?
            .unwrap_or_default();
        let len = 40 + inner.len() + intent.len();
        if len > max_bytes {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut bytes = Vec::with_capacity(len);
        bytes.extend(b"VBSRC001");
        bytes.extend(self.applied.to_le_bytes());
        bytes.extend((self.export_bytes as u64).to_le_bytes());
        bytes.extend(self.routed.applied_index().to_le_bytes());
        bytes.extend((intent.len() as u32).to_le_bytes());
        bytes.extend(intent);
        bytes.extend((inner.len() as u32).to_le_bytes());
        bytes.extend(inner);
        Ok(bytes)
    }
    fn restore_checkpoint(
        &mut self,
        schema: u64,
        applied: u64,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        if schema != TRANSFER_SOURCE_SCHEMA {
            return Err(ApplicationError::UnsupportedSchema);
        }
        if bytes.len() > self.readiness_requirements().snapshot_bytes {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBSRC001" || r.u64()? != applied || r.u64()? != self.export_bytes as u64
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let inner_applied = r.u64()?;
        let len = r.u32()? as usize;
        let intent = if len == 0 {
            None
        } else {
            Some(self.checked_intent(r.take(len)?)?)
        };
        let len = r.u32()? as usize;
        let mut next = self.clone();
        next.routed
            .restore_checkpoint(ROUTED_APPLICATION_SCHEMA, inner_applied, r.take(len)?)?;
        if !r.done()
            || inner_applied > applied
            || next.routed.fence().is_some() != intent.is_some()
            || next
                .routed
                .fence()
                .is_some_and(|f| f.index != inner_applied)
            || intent.is_none() && inner_applied != applied
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        next.intent = intent;
        next.applied = applied;
        if let Some(intent) = &next.intent {
            if next
                .fence()
                .is_none_or(|f| !intent.permits_operation(f.operation))
            {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            for (target, _) in next.export_ranges(intent)? {
                next.export_target(target, next.export_bytes)?;
            }
        }
        *self = next;
        Ok(())
    }
}
impl<A, P> ReadableStateMachine for TransferSource<A, P>
where
    A: ScopeStateMachine + BoundedStateMachine + ReadableStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    type Query = SourceQuery<A::Query>;
    type ReadResult = SourceRead<A::ReadResult>;
    fn read_at(
        &self,
        required: u64,
        query: Self::Query,
    ) -> Result<Self::ReadResult, ApplicationError> {
        if required > self.applied {
            return Err(ApplicationError::NotApplied);
        }
        match query {
            SourceQuery::Data(query) => self
                .routed
                .read_at(required.min(self.routed.applied_index()), query)
                .map(SourceRead::Data),
            SourceQuery::Freeze => {
                let status = if let Some(intent) = &self.intent {
                    let ranges = self.export_ranges(intent)?;
                    let mut exports = Vec::with_capacity(ranges.len());
                    for (target, scope) in ranges {
                        let image = self.export_target(target, self.export_bytes)?;
                        exports.push(SourceExportCommitment {
                            target,
                            scope,
                            digest: ContentDigest::scope_image(&image),
                        });
                    }
                    Some(SourceFreezeStatus {
                        fence: self.fence().expect("frozen intent has fence"),
                        intent: intent.clone(),
                        exports,
                    })
                } else {
                    None
                };
                Ok(SourceRead::Freeze(status))
            }
        }
    }
}
impl<A, P> BoundedReadableStateMachine for TransferSource<A, P>
where
    A: ScopeStateMachine + BoundedStateMachine + BoundedReadableStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    fn query_bytes(&self, query: &Self::Query, limit: usize) -> Result<usize, ApplicationError> {
        match query {
            SourceQuery::Data(q) => self.routed.query_bytes(q, limit),
            SourceQuery::Freeze => Ok(0),
        }
    }
    fn read_result_bound(&self, query: &Self::Query) -> Result<usize, ApplicationError> {
        let nested = match query {
            SourceQuery::Data(q) => self
                .routed
                .read_result_bound(q)?
                .checked_sub(size_of::<RoutedRead<A::ReadResult>>())
                .ok_or(ApplicationError::ReceiptBudget)?,
            SourceQuery::Freeze => {
                if let Some(i) = &self.intent {
                    i.retained_bytes() - size_of::<TransferIntent>()
                        + self.export_ranges(i)?.len() * size_of::<SourceExportCommitment>()
                } else {
                    0
                }
            }
        };
        size_of::<Self::ReadResult>()
            .checked_add(nested)
            .ok_or(ApplicationError::ReceiptBudget)
    }
    fn read_result_bytes(
        &self,
        result: &Self::ReadResult,
        limit: usize,
    ) -> Result<usize, ApplicationError> {
        let bytes = match result {
            SourceRead::Data(r) => self.routed.read_result_bytes(r, limit)?,
            SourceRead::Freeze(s) => s.as_ref().map_or(0, |s| {
                s.intent.retained_bytes() - size_of::<TransferIntent>()
                    + s.exports.capacity() * size_of::<SourceExportCommitment>()
            }),
        };
        if bytes > limit {
            return Err(ApplicationError::ReceiptBudget);
        }
        Ok(bytes)
    }
}

impl<A, P> crate::retirement::sealed::Sealed for TransferSource<A, P> {}
impl<A, P> crate::retirement::RetirableOwner for TransferSource<A, P>
where
    A: ScopeStateMachine + BoundedStateMachine + ProposalAdmission + BoundedReadableStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    fn retirement_requirements(&self) -> crate::raft::ReadinessRequirements {
        self.readiness_requirements()
    }
    fn retirement_source_status(&self) -> Result<Option<SourceFreezeStatus>, ApplicationError> {
        let SourceRead::Freeze(status) = self.read_at(self.applied_index(), SourceQuery::Freeze)?
        else {
            return Err(ApplicationError::InvalidCommand);
        };
        Ok(status)
    }
    fn retirement_export(
        &self,
        target: GroupIdentity,
        max_bytes: usize,
    ) -> Result<ScopeImage, ApplicationError> {
        self.export_target(target, max_bytes)
    }
    fn retirement_lineage(&self) -> Result<Vec<u8>, ApplicationError> {
        if self.fence().is_none() {
            return Err(ApplicationError::NotApplied);
        }
        Ok(Vec::new())
    }
    fn validate_retirement_source(
        &self,
        status: &SourceFreezeStatus,
    ) -> Result<(), ApplicationError> {
        self.validate_group(status.fence.group)?;
        if status.intent.before() != self.routed.grant() {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        Ok(())
    }
    fn validate_retirement_lineage(&self, bytes: &[u8], _: u64) -> Result<(), ApplicationError> {
        if !bytes.is_empty() {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        Ok(())
    }
}
