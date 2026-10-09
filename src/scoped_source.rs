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
    transfer::ContentDigest,
};
use std::{collections::BTreeMap, mem::size_of};
pub const SCOPED_TRANSFER_SOURCE_SCHEMA: u64 = 1;
pub const MAX_SCOPED_SOURCE_CHECKPOINT_BYTES: usize = 64 * 1024 * 1024;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScopedExportStatus {
    pub fence: ScopedOwnershipFence,
    pub digest: ContentDigest,
    pub schema: u64,
    pub payload_bytes: usize,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScopedSourceQuery<Q> {
    Data(RoutedQuery<Q>),
    Frozen(OperationId),
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScopedSourceRead<R> {
    Data(RoutedRead<R>),
    Frozen(Option<ScopedExportStatus>),
}
#[derive(Clone)]
struct FrozenExport {
    digest: ContentDigest,
    image: ScopeImage,
    reservation: usize,
}
/// Owns one routed application and bounded immutable provider images. No I/O or
/// additional persistence owner. Authorize fences separately from ordinary data;
/// status and exports alone never publish ownership or activate a target.
#[derive(Clone)]
pub struct ScopedTransferSource<A, P> {
    routed: RoutedApplication<A, P>,
    export_bytes: usize,
    images: BTreeMap<OperationId, FrozenExport>,
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
            routed,
            export_bytes,
            images: BTreeMap::new(),
        })
    }
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
            .checked_add(20)
            .is_none_or(|n| n > max_bytes || n > MAX_ROUTED_COMMAND_BYTES)
        {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut bytes = Vec::with_capacity(20 + inner.len());
        bytes.extend(b"VBSCOWN1");
        bytes.extend((self.export_bytes as u64).to_le_bytes());
        bytes.extend((inner.len() as u32).to_le_bytes());
        bytes.extend(inner);
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
            let replacement = if bytes.starts_with(b"VBSCOWN1") {
                if bytes != &self.bootstrap_command(bytes.len())? {
                    return Err(ApplicationError::InvalidCommand);
                }
                self.routed.bootstrap_command(bytes.len())?
            } else {
                match decode(bytes, self.routed.limits().payload_bytes)? {
                    Command::Data { key, payload, .. } => {
                        if self.routed.application().command_key(payload)? != key {
                            return Err(ApplicationError::InvalidCommand);
                        }
                    }
                    Command::ScopeFence(_, scope) => {
                        if !self.images.contains_key(operation) {
                            if self.routed.application().contains_operation(*operation) {
                                return Err(ApplicationError::InvalidCommand);
                            }
                            self.export_capacity(scope)?;
                        }
                    }
                    Command::Fence(_) => {
                        if self.routed.application().contains_operation(*operation) {
                            return Err(ApplicationError::InvalidCommand);
                        }
                    }
                    Command::Bootstrap(_) => return Err(ApplicationError::InvalidCommand),
                }
                bytes.clone()
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
        let fence = *self
            .routed
            .scoped_fences()
            .iter()
            .find(|f| f.fence.operation == operation)
            .expect("image has original fence");
        Some(ScopedExportStatus {
            fence,
            digest: frozen.digest,
            schema: image.schema(),
            payload_bytes: image.bytes().len(),
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
            application_schema: SCOPED_TRANSFER_SOURCE_SCHEMA,
            command_bytes: inner.command_bytes + 20,
            snapshot_bytes: inner.snapshot_bytes
                + 30
                + 100 * self.routed.scoped_fence_limit()
                + self.export_bytes,
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
            let projected = next.projected(entry)?;
            let applied = next.routed.apply_batch(std::slice::from_ref(&projected))?;
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
                            },
                        );
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
        SCOPED_TRANSFER_SOURCE_SCHEMA
    }
    fn checkpoint(&self, max_bytes: usize) -> Result<Vec<u8>, ApplicationError> {
        let inner = self.routed.checkpoint(max_bytes)?;
        let len = 30
            + inner.len()
            + self
                .images
                .values()
                .map(|e| 100 + e.image.bytes().len())
                .sum::<usize>();
        if len > max_bytes || len > self.readiness_requirements().snapshot_bytes {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut out = Vec::with_capacity(len);
        out.extend(b"VBSCCHK1");
        out.extend(self.applied_index().to_le_bytes());
        out.extend((self.export_bytes as u64).to_le_bytes());
        out.extend((inner.len() as u32).to_le_bytes());
        out.extend(inner);
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
        }
        Ok(out)
    }
    fn restore_checkpoint(
        &mut self,
        schema: u64,
        applied: u64,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        if schema != SCOPED_TRANSFER_SOURCE_SCHEMA {
            return Err(ApplicationError::UnsupportedSchema);
        }
        if bytes.len() > self.readiness_requirements().snapshot_bytes {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let restore = || -> Result<Self, ApplicationError> {
            let mut r = Reader::new(bytes);
            if r.take(8)? != b"VBSCCHK1"
                || r.u64()? != applied
                || r.u64()? != self.export_bytes as u64
            {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            let len = r.u32()? as usize;
            let mut next = self.clone();
            next.routed.restore_checkpoint(
                SCOPED_ROUTED_APPLICATION_SCHEMA,
                applied,
                r.take(len)?,
            )?;
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
                    },
                );
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
            ScopedSourceQuery::Data(q) => self.routed.read_at(index, q).map(ScopedSourceRead::Data),
            ScopedSourceQuery::Frozen(op) => Ok(ScopedSourceRead::Frozen(self.status(op))),
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
            ScopedSourceQuery::Frozen(_) => Ok(0),
        }
    }
    fn read_result_bound(&self, q: &Self::Query) -> Result<usize, ApplicationError> {
        let nested = match q {
            ScopedSourceQuery::Data(q) => self
                .routed
                .read_result_bound(q)?
                .checked_sub(size_of::<RoutedRead<A::ReadResult>>())
                .ok_or(ApplicationError::ReceiptBudget)?,
            ScopedSourceQuery::Frozen(_) => 0,
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
            ScopedSourceRead::Frozen(_) => Ok(0),
        }
    }
}
