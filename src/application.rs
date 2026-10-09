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
//! Ordered application seam and a deterministic counter with operation retries.
use crate::{
    identity::{GroupIdentity, OperationId},
    log::{EntryPayload, LogEntry},
    raft::{Raft, RaftError, ReadBarrier},
};
use std::collections::{BTreeMap, BTreeSet};

pub const PROPOSAL_ADMISSION_CONTRACT_VERSION: u32 = 1;

/// Deterministic application admission against all unapplied durable commands
/// and retained queued/in-flight client commands. The caller serializes this
/// check with application and routes every service proposal through it.
/// Return a per-command receipt retention bound (including inline receipt).
/// Validation changes no state and performs no I/O; accepted capacity remains
/// represented by the caller's pending commands until log/application takeover.
pub trait ProposalAdmission: BoundedStateMachine
where
    Self::Receipt: ApplicationReceipt,
{
    fn validate_proposal<'a>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError>;
}

/// Optional bounded receipt capability; wire and checkpoint schemas are separate.
pub const BOUNDED_APPLICATION_CONTRACT_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CounterOutcome {
    Value(i64),
    Overflow,
    OperationConflict,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CounterReceipt {
    pub index: u64,
    pub operation: OperationId,
    pub outcome: CounterOutcome,
    pub duplicate: bool,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationError {
    IndexGap,
    InvalidCommand,
    DedupCapacity,
    ReceiptBudget,
    NotApplied,
    InvalidCheckpoint,
    UnsupportedSchema,
}

/// A host applies only committed, contiguous entries and emits client success
/// after application. Local reads do not create a linearizability proof; the
/// core's read-barrier protocol must authorize distributed linearizable reads.
/// Application checkpoints and restore join this seam with snapshot support.
pub trait StateMachine {
    type Receipt;
    /// Validate the concrete group before assembly, execution or recovery.
    /// Generic applications may accept any group; ownership-bound applications
    /// must reject another group without changing state or performing I/O.
    fn validate_group(&self, _group: GroupIdentity) -> Result<(), ApplicationError> {
        Ok(())
    }
    /// Enforced whole configured lifetime bounds, including retained retries and
    /// lifecycle metadata. Current occupancy/checkpoint size is insufficient.
    /// None preserves ordinary execution but refuses membership/readiness use.
    /// Reporting bounds is a trusted provider assertion, not a reservation.
    fn deployment_requirements(&self) -> Option<crate::raft::ReadinessRequirements> {
        None
    }
    /// Pure, nonblocking check against the selected application's enforced bounds.
    fn validate_deployment_requirements(
        &self,
        declared: crate::raft::ReadinessRequirements,
    ) -> Result<(), ApplicationError> {
        let actual = self
            .deployment_requirements()
            .ok_or(ApplicationError::InvalidCheckpoint)?;
        if actual.application_schema == 0
            || actual.command_bytes == 0
            || actual.snapshot_bytes == 0
            || declared.command_bytes == 0
            || declared.snapshot_bytes == 0
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        if declared.application_schema != actual.application_schema {
            return Err(ApplicationError::UnsupportedSchema);
        }
        if declared.command_bytes < actual.command_bytes
            || declared.snapshot_bytes < actual.snapshot_bytes
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        Ok(())
    }
    fn applied_index(&self) -> u64;
    fn apply_batch(&mut self, entries: &[LogEntry])
        -> Result<Vec<Self::Receipt>, ApplicationError>;
}

/// One ordered command outcome. Nested retained capacity excludes the inline
/// receipt itself, which the result router charges through vector capacity.
/// Host implementations are trusted to report their actual allocations.
pub trait ApplicationReceipt {
    fn index(&self) -> u64;
    fn operation(&self) -> OperationId;
    fn nested_bytes(&self, limit: usize) -> Result<usize, ApplicationError>;
}

/// Optional bounded result capability over the ordinary application seam.
/// Exactly one receipt per Command, in input order; no receipt for other entries.
/// The bound includes vector spare capacity and all nested retained allocations.
/// Computing it must not mutate the application or perform blocking I/O.
/// Successful apply advances exactly through the batch's last index. Failure
/// must leave state unchanged; the router nevertheless fences the owner on an
/// application failure because a generic host cannot prove partial progress away.
pub trait BoundedStateMachine: StateMachine
where
    Self::Receipt: ApplicationReceipt,
{
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError>;
}

impl ApplicationReceipt for CounterReceipt {
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

impl BoundedStateMachine for Counter {
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        entries
            .iter()
            .filter(|e| matches!(e.payload, EntryPayload::Command { .. }))
            .count()
            .checked_mul(std::mem::size_of::<CounterReceipt>())
            .ok_or(ApplicationError::InvalidCommand)
    }
}

impl ProposalAdmission for Counter {
    fn validate_proposal<'a>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError> {
        if bytes.len() != 8 {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut reserved = BTreeSet::new();
        for (id, _) in pending {
            if !self.dedup.contains_key(&id) {
                reserved.insert(id);
            }
            if reserved.len() > self.remaining_operations() {
                return Err(ApplicationError::DedupCapacity);
            }
        }
        if !self.dedup.contains_key(&operation) {
            reserved.insert(operation);
        }
        if reserved.len() > self.remaining_operations() {
            return Err(ApplicationError::DedupCapacity);
        }
        Ok(std::mem::size_of::<CounterReceipt>())
    }
}

/// A checkpoint contains every piece of state needed for deterministic replay,
/// including operation identities/content/outcomes and its applied boundary.
/// Restore must reject unsupported schemas and incompatible configured capacity.
pub trait CheckpointStateMachine: StateMachine + Clone {
    fn schema_version(&self) -> u64;
    fn checkpoint(&self, max_bytes: usize) -> Result<Vec<u8>, ApplicationError>;
    fn restore_checkpoint(
        &mut self,
        schema: u64,
        applied_index: u64,
        bytes: &[u8],
    ) -> Result<(), ApplicationError>;
}

/// Read support is explicit and optional. `read_at` must observe immutable
/// applied state and reject a boundary it has not applied. Hosts keep the group
/// owner and its application serialized while invoking `read_at_barrier`.
pub trait ReadableStateMachine: StateMachine {
    type Query;
    type ReadResult;
    fn read_at(
        &self,
        required_index: u64,
        query: Self::Query,
    ) -> Result<Self::ReadResult, ApplicationError>;
}

/// Optional bounded read execution contract, version 1. Byte counts include
/// retained allocation capacity, not just logical length. Bounds include inline
/// result storage; queries report nested storage only. Implementations must not
/// perform I/O or mutate applied state during validation or read execution.
pub trait BoundedReadableStateMachine: ReadableStateMachine {
    fn query_bytes(&self, query: &Self::Query, limit: usize) -> Result<usize, ApplicationError>;
    fn read_result_bound(&self, query: &Self::Query) -> Result<usize, ApplicationError>;
    fn read_result_bytes(
        &self,
        result: &Self::ReadResult,
        nested_limit: usize,
    ) -> Result<usize, ApplicationError>;
}

pub const BOUNDED_READ_CONTRACT_VERSION: u32 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadError {
    Consensus(RaftError),
    Application(ApplicationError),
}

/// Serve the original pending invocation after a quorum and application catchup.
/// The barrier is consumed once, including when the application rejects a query.
pub fn read_at_barrier<A: ReadableStateMachine>(
    raft: &mut Raft,
    barrier: &ReadBarrier,
    application: &A,
    query: A::Query,
) -> Result<A::ReadResult, ReadError> {
    raft.finish_read(barrier, application.applied_index())
        .map_err(ReadError::Consensus)?;
    application
        .validate_group(raft.state().bootstrap.group)
        .map_err(ReadError::Application)?;
    application
        .read_at(barrier.index(), query)
        .map_err(ReadError::Application)
}

#[derive(Clone)]
pub struct Counter {
    value: i64,
    applied: u64,
    max_operations: usize,
    dedup: BTreeMap<OperationId, ([u8; 8], CounterOutcome)>,
}
impl Counter {
    /// Capacity is an application-schema parameter, identical on all replicas.
    /// It must not be changed silently on restore. No implicit eviction occurs.
    pub fn new(max_operations: usize) -> Result<Self, ApplicationError> {
        if max_operations == 0 || max_operations > 1_000_000 {
            return Err(ApplicationError::DedupCapacity);
        }
        Ok(Self {
            value: 0,
            applied: 0,
            max_operations,
            dedup: BTreeMap::new(),
        })
    }
    /// Diagnostic/local applied read, not a distributed read-index API.
    pub fn read_applied(&self, required_index: u64) -> Result<i64, ApplicationError> {
        if self.applied < required_index {
            return Err(ApplicationError::NotApplied);
        }
        Ok(self.value)
    }
    pub fn remaining_operations(&self) -> usize {
        self.max_operations - self.dedup.len()
    }
    /// Whole configured lifetime envelope, including every retained retry
    /// outcome. Admission and restore enforce this capacity; it is independent
    /// of current occupancy and must not shrink when the application is empty.
    pub fn readiness_requirements(&self) -> crate::raft::ReadinessRequirements {
        crate::raft::ReadinessRequirements {
            application_schema: self.schema_version(),
            command_bytes: 8,
            snapshot_bytes: 32 + 33 * self.max_operations,
        }
    }
    /// Check an administrator's declaration against enforced application bounds.
    /// Larger byte envelopes are allowed; the schema must match exactly.
    pub fn validate_readiness_requirements(
        &self,
        declared: crate::raft::ReadinessRequirements,
    ) -> Result<(), ApplicationError> {
        let actual = self.readiness_requirements();
        if declared.application_schema != actual.application_schema {
            return Err(ApplicationError::UnsupportedSchema);
        }
        if declared.command_bytes < actual.command_bytes
            || declared.snapshot_bytes < actual.snapshot_bytes
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        Ok(())
    }
}
impl StateMachine for Counter {
    type Receipt = CounterReceipt;
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
        // A bounded clone keeps a malformed/gapped batch from partly advancing
        // the applied state. Checkpoints preserve both state and dedup outcomes.
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
                let delta: [u8; 8] = bytes
                    .as_slice()
                    .try_into()
                    .map_err(|_| ApplicationError::InvalidCommand)?;
                let (outcome, duplicate) =
                    if let Some((original, result)) = next.dedup.get(operation) {
                        (
                            if *original == delta {
                                *result
                            } else {
                                CounterOutcome::OperationConflict
                            },
                            true,
                        )
                    } else {
                        if next.dedup.len() >= next.max_operations {
                            return Err(ApplicationError::DedupCapacity);
                        }
                        let outcome = match next.value.checked_add(i64::from_le_bytes(delta)) {
                            Some(value) => {
                                next.value = value;
                                CounterOutcome::Value(value)
                            }
                            None => CounterOutcome::Overflow,
                        };
                        next.dedup.insert(*operation, (delta, outcome));
                        (outcome, false)
                    };
                receipts.push(CounterReceipt {
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

impl ReadableStateMachine for Counter {
    type Query = ();
    type ReadResult = i64;
    fn read_at(&self, required_index: u64, (): ()) -> Result<i64, ApplicationError> {
        self.read_applied(required_index)
    }
}
impl BoundedReadableStateMachine for Counter {
    fn query_bytes(&self, (): &(), _: usize) -> Result<usize, ApplicationError> {
        Ok(0)
    }
    fn read_result_bound(&self, (): &()) -> Result<usize, ApplicationError> {
        Ok(std::mem::size_of::<i64>())
    }
    fn read_result_bytes(&self, _: &i64, _: usize) -> Result<usize, ApplicationError> {
        Ok(0)
    }
}

impl CheckpointStateMachine for Counter {
    fn schema_version(&self) -> u64 {
        1
    }
    fn checkpoint(&self, max_bytes: usize) -> Result<Vec<u8>, ApplicationError> {
        let length = 32 + 33 * self.dedup.len();
        if length > max_bytes {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut bytes = Vec::with_capacity(length);
        bytes.extend(b"VBCTR001");
        bytes.extend(self.applied.to_le_bytes());
        bytes.extend((self.max_operations as u32).to_le_bytes());
        bytes.extend(self.value.to_le_bytes());
        bytes.extend((self.dedup.len() as u32).to_le_bytes());
        for (id, (request, outcome)) in &self.dedup {
            bytes.extend(id.get().to_le_bytes());
            bytes.extend(request);
            match outcome {
                CounterOutcome::Value(v) => {
                    bytes.push(0);
                    bytes.extend(v.to_le_bytes());
                }
                CounterOutcome::Overflow => {
                    bytes.push(1);
                    bytes.extend([0; 8]);
                }
                CounterOutcome::OperationConflict => {
                    return Err(ApplicationError::InvalidCheckpoint)
                }
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
        if bytes.len() < 32 || &bytes[..8] != b"VBCTR001" {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let index = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let capacity = u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize;
        let value = i64::from_le_bytes(bytes[20..28].try_into().unwrap());
        let count = u32::from_le_bytes(bytes[28..32].try_into().unwrap()) as usize;
        if index != applied
            || capacity != self.max_operations
            || count > capacity
            || count as u64 > index
            || bytes.len() != 32 + 33 * count
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut dedup = BTreeMap::new();
        let mut previous = 0;
        for record in bytes[32..].as_chunks::<33>().0 {
            let id = u128::from_le_bytes(record[..16].try_into().unwrap());
            if id <= previous {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            previous = id;
            let request = record[16..24].try_into().unwrap();
            let outcome = match record[24] {
                0 => CounterOutcome::Value(i64::from_le_bytes(record[25..33].try_into().unwrap())),
                1 if record[25..33] == [0; 8] => CounterOutcome::Overflow,
                _ => return Err(ApplicationError::InvalidCheckpoint),
            };
            dedup.insert(
                OperationId::new(id).ok_or(ApplicationError::InvalidCheckpoint)?,
                (request, outcome),
            );
        }
        self.value = value;
        self.applied = index;
        self.dedup = dedup;
        Ok(())
    }
}
