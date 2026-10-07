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
    identity::OperationId,
    log::{EntryPayload, LogEntry},
    raft::{Raft, RaftError, ReadBarrier},
};
use std::collections::BTreeMap;

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
    NotApplied,
}

/// A host applies only committed, contiguous entries and emits client success
/// after application. Local reads do not create a linearizability proof; the
/// core's read-barrier protocol must authorize distributed linearizable reads.
/// Application checkpoints and restore join this seam with snapshot support.
pub trait StateMachine {
    type Receipt;
    fn applied_index(&self) -> u64;
    fn apply_batch(&mut self, entries: &[LogEntry])
        -> Result<Vec<Self::Receipt>, ApplicationError>;
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
}
impl StateMachine for Counter {
    type Receipt = CounterReceipt;
    fn applied_index(&self) -> u64 {
        self.applied
    }
    fn apply_batch(
        &mut self,
        entries: &[LogEntry],
    ) -> Result<Vec<Self::Receipt>, ApplicationError> {
        // A bounded clone keeps a malformed/gapped batch from partly advancing
        // the applied state. The future checkpoint contract records both parts.
        let mut next = self.clone();
        let mut receipts = Vec::new();
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
