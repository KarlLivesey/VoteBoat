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
//! Native deterministic per-bucket counters with transferable retries and outbox.
//! Scope import is a data mechanism; its caller supplies the lifecycle protocol.
use crate::{application::*, identity::*, log::*, routing::codec::*, routing::*, scope::*};
use std::{collections::BTreeMap, mem::size_of};

pub const BUCKET_COUNTER_SCHEMA: u64 = 1;
pub const MAX_BUCKET_OPERATIONS: usize = 4096;
pub const MAX_BUCKET_OUTBOX_BYTES: usize = 65536;
pub const MAX_BUCKET_COMMAND_BYTES: usize = 24 + MAX_ROUTING_KEY_BYTES + MAX_BUCKET_OUTBOX_BYTES;
const HEADER: usize = 56;
const RECORD: usize = 29;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BucketCounterLimits {
    pub operations: usize,
    /// Retained exact command bytes, including keys and optional outbox payloads.
    pub semantic_bytes: usize,
}
#[derive(Debug)]
pub struct BucketCounterRejected<P> {
    pub error: ApplicationError,
    pub scope: BucketRange,
    pub policy: P,
    pub limits: BucketCounterLimits,
}
impl BucketCounterLimits {
    fn valid(self) -> bool {
        self.operations > 0
            && self.operations <= MAX_BUCKET_OPERATIONS
            && self.semantic_bytes > 0
            && self.semantic_bytes
                <= MAX_SCOPE_IMAGE_BYTES - HEADER - 2048 - RECORD * self.operations
    }
    pub fn checkpoint_bound(self) -> Result<usize, ApplicationError> {
        if !self.valid() {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        Ok(HEADER + 2048 + RECORD * self.operations + self.semantic_bytes)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BucketOutcome {
    Value(i64),
    Overflow,
    OperationConflict,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BucketReceipt {
    pub index: u64,
    pub operation: OperationId,
    pub outcome: BucketOutcome,
    pub duplicate: bool,
}
impl ApplicationReceipt for BucketReceipt {
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
struct Record {
    command: Vec<u8>,
    outcome: BucketOutcome,
}
#[derive(Clone)]
pub struct BucketCounter<P> {
    scope: BucketRange,
    policy: P,
    limits: BucketCounterLimits,
    applied: u64,
    values: [i64; 256],
    records: BTreeMap<OperationId, Record>,
    semantic_bytes: usize,
}
struct Command<'a> {
    key: &'a [u8],
    delta: i64,
    outbox: &'a [u8],
}
fn command(bytes: &[u8]) -> Result<Command<'_>, ApplicationError> {
    if bytes.len() > MAX_BUCKET_COMMAND_BYTES {
        return Err(ApplicationError::InvalidCommand);
    }
    let mut r = Reader::new(bytes);
    if r.take(8)? != b"VBBCMD01" {
        return Err(ApplicationError::InvalidCommand);
    }
    let length = r.u32()? as usize;
    if length == 0 || length > MAX_ROUTING_KEY_BYTES {
        return Err(ApplicationError::InvalidCommand);
    }
    let key = r.take(length)?;
    let delta = i64::from_le_bytes(r.take(8)?.try_into().unwrap());
    let length = r.u32()? as usize;
    if length > MAX_BUCKET_OUTBOX_BYTES {
        return Err(ApplicationError::InvalidCommand);
    }
    let outbox = r.take(length)?;
    if !r.done() {
        return Err(ApplicationError::InvalidCommand);
    }
    Ok(Command { key, delta, outbox })
}
/// A nonempty outbox payload is atomically queued only on a successful addition.
/// Its stable delivery ID is the command operation ID. Replay never delivers it.
pub fn encode_add(
    key: &[u8],
    delta: i64,
    outbox: &[u8],
    max_bytes: usize,
) -> Result<Vec<u8>, ApplicationError> {
    let len = 24usize
        .checked_add(key.len())
        .and_then(|n| n.checked_add(outbox.len()))
        .ok_or(ApplicationError::InvalidCommand)?;
    if key.is_empty()
        || key.len() > MAX_ROUTING_KEY_BYTES
        || outbox.len() > MAX_BUCKET_OUTBOX_BYTES
        || len > max_bytes
    {
        return Err(ApplicationError::InvalidCommand);
    }
    let mut bytes = Vec::with_capacity(len);
    bytes.extend(b"VBBCMD01");
    bytes.extend((key.len() as u32).to_le_bytes());
    bytes.extend(key);
    bytes.extend(delta.to_le_bytes());
    bytes.extend((outbox.len() as u32).to_le_bytes());
    bytes.extend(outbox);
    Ok(bytes)
}
impl<P: PartitionPolicy + Clone> BucketCounter<P> {
    pub fn new(
        scope: BucketRange,
        policy: P,
        limits: BucketCounterLimits,
    ) -> Result<Self, BucketCounterRejected<P>> {
        if !limits.valid() || policy.scheme().version == 0 {
            return Err(BucketCounterRejected {
                error: ApplicationError::InvalidCommand,
                scope,
                policy,
                limits,
            });
        }
        Ok(Self {
            scope,
            policy,
            limits,
            applied: 0,
            values: [0; 256],
            records: BTreeMap::new(),
            semantic_bytes: 0,
        })
    }
    fn bucket(&self, key: &[u8], scope: BucketRange) -> Result<usize, ApplicationError> {
        if key.len() > MAX_ROUTING_KEY_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let bucket = self
            .policy
            .bucket(key)
            .map_err(|_| ApplicationError::InvalidCommand)?;
        if !scope.contains(bucket) {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(usize::from(bucket))
    }
    pub fn value(&self, key: &[u8]) -> Result<i64, ApplicationError> {
        Ok(self.values[self.bucket(key, self.scope)?])
    }
    /// Local pending instructions, not a delivery acknowledgment. The recipient
    /// must deduplicate operation IDs. This bounded version retains them for life.
    pub fn outbox(&self) -> impl Iterator<Item = (OperationId, &[u8], &[u8])> {
        self.records.iter().filter_map(|(id, record)| {
            let c = command(&record.command).expect("validated command");
            (matches!(record.outcome, BucketOutcome::Value(_)) && !c.outbox.is_empty())
                .then_some((*id, c.key, c.outbox))
        })
    }
    pub fn readiness_requirements(&self) -> crate::raft::ReadinessRequirements {
        crate::raft::ReadinessRequirements {
            application_schema: BUCKET_COUNTER_SCHEMA,
            command_bytes: MAX_BUCKET_COMMAND_BYTES,
            snapshot_bytes: self.limits.checkpoint_bound().expect("validated limits"),
        }
    }
    fn apply_command(
        &mut self,
        operation: OperationId,
        bytes: &[u8],
    ) -> Result<(BucketOutcome, bool), ApplicationError> {
        let c = command(bytes)?;
        let bucket = self.bucket(c.key, self.scope)?;
        if let Some(old) = self.records.get(&operation) {
            return Ok((
                if old.command == bytes {
                    old.outcome
                } else {
                    BucketOutcome::OperationConflict
                },
                true,
            ));
        }
        if self.records.len() >= self.limits.operations
            || bytes.len() > self.limits.semantic_bytes - self.semantic_bytes
        {
            return Err(ApplicationError::DedupCapacity);
        }
        let owned = bytes.to_vec();
        if owned.capacity() > self.limits.semantic_bytes - self.semantic_bytes {
            return Err(ApplicationError::DedupCapacity);
        }
        let outcome = if let Some(value) = self.values[bucket].checked_add(c.delta) {
            self.values[bucket] = value;
            BucketOutcome::Value(value)
        } else {
            BucketOutcome::Overflow
        };
        self.semantic_bytes += owned.capacity();
        self.records.insert(
            operation,
            Record {
                command: owned,
                outcome,
            },
        );
        Ok((outcome, false))
    }
    fn image(&self, scope: BucketRange, max_bytes: usize) -> Result<Vec<u8>, ApplicationError> {
        if scope.start() < self.scope.start() || scope.end() > self.scope.end() {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let records: Vec<_> = self
            .records
            .iter()
            .filter(|(_, r)| self.bucket(command(&r.command).unwrap().key, scope).is_ok())
            .collect();
        let len = HEADER
            + 8 * usize::from(scope.end() - scope.start())
            + records
                .iter()
                .map(|(_, r)| RECORD + r.command.len())
                .sum::<usize>();
        if len > max_bytes || len > MAX_SCOPE_IMAGE_BYTES {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut b = Vec::with_capacity(len);
        b.extend(b"VBBCP001");
        b.extend(self.applied.to_le_bytes());
        put_range(&mut b, scope);
        b.extend(self.policy.scheme().id.get().to_le_bytes());
        b.extend(self.policy.scheme().version.to_le_bytes());
        b.extend((self.limits.operations as u32).to_le_bytes());
        b.extend((self.limits.semantic_bytes as u64).to_le_bytes());
        b.extend((records.len() as u32).to_le_bytes());
        for value in &self.values[usize::from(scope.start())..usize::from(scope.end())] {
            b.extend(value.to_le_bytes());
        }
        for (id, record) in records {
            b.extend(id.get().to_le_bytes());
            b.extend((record.command.len() as u32).to_le_bytes());
            b.extend(&record.command);
            match record.outcome {
                BucketOutcome::Value(value) => {
                    b.push(0);
                    b.extend(value.to_le_bytes());
                }
                BucketOutcome::Overflow => {
                    b.push(1);
                    b.extend(0i64.to_le_bytes());
                }
                BucketOutcome::OperationConflict => unreachable!("not retained"),
            }
        }
        Ok(b)
    }
    fn decode_image(&self, bytes: &[u8]) -> Result<Self, ApplicationError> {
        if bytes.len() > MAX_SCOPE_IMAGE_BYTES {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBBCP001" {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let applied = r.u64()?;
        let scope = r.range()?;
        let scheme = PartitionScheme {
            id: RoutingSchemeId::new(r.u128()?).ok_or(ApplicationError::InvalidCheckpoint)?,
            version: r.u32()?,
        };
        if scheme != self.policy.scheme() {
            return Err(ApplicationError::UnsupportedSchema);
        }
        let limits = BucketCounterLimits {
            operations: r.u32()? as usize,
            semantic_bytes: usize::try_from(r.u64()?)
                .map_err(|_| ApplicationError::InvalidCheckpoint)?,
        };
        let count = r.u32()? as usize;
        if !limits.valid() || count > limits.operations {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut next = Self::new(scope, self.policy.clone(), limits).map_err(|r| r.error)?;
        next.applied = applied;
        for value in &mut next.values[usize::from(scope.start())..usize::from(scope.end())] {
            *value = i64::from_le_bytes(r.take(8)?.try_into().unwrap());
        }
        let mut previous = 0;
        for _ in 0..count {
            let operation = r.operation()?;
            if operation.get() <= previous {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            previous = operation.get();
            let len = r.u32()? as usize;
            let bytes = r.take(len)?;
            let c = command(bytes)?;
            next.bucket(c.key, scope)?;
            let tag = r.u8()?;
            let value = i64::from_le_bytes(r.take(8)?.try_into().unwrap());
            let outcome = match (tag, value) {
                (0, v) => BucketOutcome::Value(v),
                (1, 0) => BucketOutcome::Overflow,
                _ => return Err(ApplicationError::InvalidCheckpoint),
            };
            if bytes.len() > limits.semantic_bytes - next.semantic_bytes {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            let owned = bytes.to_vec();
            if owned.capacity() > limits.semantic_bytes - next.semantic_bytes {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            next.semantic_bytes += owned.capacity();
            next.records.insert(
                operation,
                Record {
                    command: owned,
                    outcome,
                },
            );
        }
        if !r.done() {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        Ok(next)
    }
}
impl<P: PartitionPolicy + Clone> StateMachine for BucketCounter<P> {
    type Receipt = BucketReceipt;
    fn applied_index(&self) -> u64 {
        self.applied
    }
    fn apply_batch(
        &mut self,
        entries: &[LogEntry],
    ) -> Result<Vec<BucketReceipt>, ApplicationError> {
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
                let (outcome, duplicate) = next.apply_command(*operation, bytes)?;
                receipts.push(BucketReceipt {
                    index: entry.index,
                    operation: *operation,
                    outcome,
                    duplicate,
                });
            }
            next.applied = entry.index;
        }
        *self = next;
        Ok(receipts)
    }
}
impl<P: PartitionPolicy + Clone> BoundedStateMachine for BucketCounter<P> {
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        entries
            .iter()
            .filter(|e| matches!(e.payload, EntryPayload::Command { .. }))
            .count()
            .checked_mul(size_of::<BucketReceipt>())
            .ok_or(ApplicationError::ReceiptBudget)
    }
}
impl<P: PartitionPolicy + Clone> ProposalAdmission for BucketCounter<P> {
    fn validate_proposal<'a>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError> {
        let mut next = self.clone();
        for (count, (operation, bytes)) in pending.enumerate() {
            if count == 8192 {
                return Err(ApplicationError::DedupCapacity);
            }
            next.apply_command(operation, bytes)?;
        }
        next.apply_command(operation, bytes)?;
        Ok(size_of::<BucketReceipt>())
    }
}
impl<P: PartitionPolicy + Clone> ReadableStateMachine for BucketCounter<P> {
    type Query = Vec<u8>;
    type ReadResult = i64;
    fn read_at(&self, required_index: u64, key: Vec<u8>) -> Result<i64, ApplicationError> {
        if self.applied < required_index {
            return Err(ApplicationError::NotApplied);
        }
        self.value(&key)
    }
}
impl<P: PartitionPolicy + Clone> BoundedReadableStateMachine for BucketCounter<P> {
    fn query_bytes(&self, key: &Vec<u8>, limit: usize) -> Result<usize, ApplicationError> {
        self.bucket(key, self.scope)?;
        if key.capacity() > limit || key.capacity() > MAX_ROUTING_KEY_BYTES {
            return Err(ApplicationError::ReceiptBudget);
        }
        Ok(key.capacity())
    }
    fn read_result_bound(&self, key: &Vec<u8>) -> Result<usize, ApplicationError> {
        self.bucket(key, self.scope)?;
        Ok(size_of::<i64>())
    }
    fn read_result_bytes(&self, _: &i64, _: usize) -> Result<usize, ApplicationError> {
        Ok(0)
    }
}
impl<P: PartitionPolicy + Clone> CheckpointStateMachine for BucketCounter<P> {
    fn schema_version(&self) -> u64 {
        BUCKET_COUNTER_SCHEMA
    }
    fn checkpoint(&self, max_bytes: usize) -> Result<Vec<u8>, ApplicationError> {
        self.image(self.scope, max_bytes)
    }
    fn restore_checkpoint(
        &mut self,
        schema: u64,
        applied: u64,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        if schema != BUCKET_COUNTER_SCHEMA {
            return Err(ApplicationError::UnsupportedSchema);
        }
        let next = self.decode_image(bytes)?;
        if next.applied != applied || next.scope != self.scope || next.limits != self.limits {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        *self = next;
        Ok(())
    }
}
impl<P: PartitionPolicy + Clone> ScopeStateMachine for BucketCounter<P> {
    fn scope(&self) -> BucketRange {
        self.scope
    }
    fn scheme(&self) -> PartitionScheme {
        self.policy.scheme()
    }
    fn command_key<'a>(&self, bytes: &'a [u8]) -> Result<&'a [u8], ApplicationError> {
        Ok(command(bytes)?.key)
    }
    fn contains_operation(&self, operation: OperationId) -> bool {
        self.records.contains_key(&operation)
    }
    fn export_scope_bound(&self, scope: BucketRange) -> Result<usize, ApplicationError> {
        if scope.start() < self.scope.start() || scope.end() > self.scope.end() {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        self.limits.checkpoint_bound()
    }
    fn export_scope(
        &self,
        scope: BucketRange,
        max_bytes: usize,
    ) -> Result<ScopeImage, ApplicationError> {
        ScopeImage::new(
            BUCKET_COUNTER_SCHEMA,
            self.policy.scheme(),
            scope,
            self.applied,
            self.image(scope, max_bytes)?,
        )
        .map_err(|e| e.0)
    }
    fn import_scopes(
        &mut self,
        images: &[ScopeImage],
        target_index: u64,
    ) -> Result<(), ApplicationError> {
        if self.applied.checked_add(1) != Some(target_index)
            || !self.records.is_empty()
            || self.values.iter().any(|v| *v != 0)
            || images.is_empty()
            || images.len() > MAX_SCOPE_IMPORTS
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut next = self.clone();
        let mut boundary = self.scope.start();
        let mut total = 0usize;
        for image in images {
            total = total
                .checked_add(image.bytes().len())
                .ok_or(ApplicationError::InvalidCheckpoint)?;
            if total > MAX_SCOPE_IMAGE_BYTES
                || image.schema() != BUCKET_COUNTER_SCHEMA
                || image.scheme() != self.policy.scheme()
                || image.scope().start() != boundary
                || image.scope().end() > self.scope.end()
            {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            let imported = self.decode_image(image.bytes())?;
            if imported.scope != image.scope() || imported.applied != image.source_applied() {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            boundary = image.scope().end();
            for bucket in image.scope().start()..image.scope().end() {
                next.values[usize::from(bucket)] = imported.values[usize::from(bucket)];
            }
            for (id, record) in imported.records {
                if next.records.contains_key(&id) {
                    return Err(ApplicationError::InvalidCheckpoint);
                }
                if next.records.len() >= next.limits.operations
                    || record.command.capacity() > next.limits.semantic_bytes - next.semantic_bytes
                {
                    return Err(ApplicationError::DedupCapacity);
                }
                next.semantic_bytes += record.command.capacity();
                next.records.insert(id, record);
            }
        }
        if boundary != self.scope.end() {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        next.applied = target_index;
        *self = next;
        Ok(())
    }
}
