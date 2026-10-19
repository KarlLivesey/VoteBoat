// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Test-only bounded response encoding around the ordinary production Counter.
use std::{cell::Cell, rc::Rc};
use voteboat::{application::*, identity::*, log::LogEntry, runtime::LogicalResponseCodec};
#[derive(Clone)]
pub struct App(pub Counter, Rc<Cell<usize>>, Rc<Cell<bool>>);
impl App {
    pub fn new() -> Self {
        Self(
            Counter::new(16).unwrap(),
            Rc::new(Cell::new(0)),
            Rc::new(Cell::new(false)),
        )
    }
    pub fn read_calls(&self) -> usize {
        self.1.get()
    }
    pub fn fail_read_encoding(&self) {
        self.2.set(true);
    }
}
impl StateMachine for App {
    type Receipt = CounterReceipt;
    fn applied_index(&self) -> u64 {
        self.0.applied_index()
    }
    fn apply_batch(
        &mut self,
        entries: &[LogEntry],
    ) -> Result<Vec<CounterReceipt>, ApplicationError> {
        self.0.apply_batch(entries)
    }
    fn deployment_requirements(&self) -> Option<voteboat::raft::ReadinessRequirements> {
        self.0.deployment_requirements()
    }
    fn retention(&self) -> voteboat::application::Retention {
        self.0.retention()
    }
}
impl BoundedStateMachine for App {
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        self.0.receipt_bytes_bound(entries)
    }
}
impl ProposalAdmission for App {
    fn validate_proposal<'a>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError> {
        self.0.validate_proposal(operation, bytes, pending)
    }
}
impl ReadableStateMachine for App {
    type Query = ();
    type ReadResult = i64;
    fn read_at(&self, required: u64, query: ()) -> Result<i64, ApplicationError> {
        self.1.set(self.1.get() + 1);
        self.0.read_at(required, query)
    }
}
impl BoundedReadableStateMachine for App {
    fn query_bytes(&self, query: &(), limit: usize) -> Result<usize, ApplicationError> {
        self.0.query_bytes(query, limit)
    }
    fn read_result_bound(&self, query: &()) -> Result<usize, ApplicationError> {
        self.0.read_result_bound(query)
    }
    fn read_result_bytes(&self, result: &i64, limit: usize) -> Result<usize, ApplicationError> {
        self.0.read_result_bytes(result, limit)
    }
}
impl CheckpointStateMachine for App {
    fn schema_version(&self) -> u64 {
        self.0.schema_version()
    }
    fn checkpoint(&self, max: usize) -> Result<Vec<u8>, ApplicationError> {
        self.0.checkpoint(max)
    }
    fn restore_checkpoint(
        &mut self,
        schema: u64,
        index: u64,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        self.0.restore_checkpoint(schema, index, bytes)
    }
}
impl LogicalResponseCodec for App {
    fn logical_response_schema(&self) -> u64 {
        1
    }
    fn receipt_encoding_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        self.0.receipt_bytes_bound(entries)?;
        Ok(34)
    }
    fn query_encoding_bound(&self, _: &()) -> Result<usize, ApplicationError> {
        Ok(0)
    }
    fn read_encoding_bound(&self, _: &()) -> Result<usize, ApplicationError> {
        Ok(8)
    }
    fn encode_receipt(
        &self,
        receipt: &CounterReceipt,
        out: &mut [u8],
    ) -> Result<usize, ApplicationError> {
        let out = out.get_mut(..34).ok_or(ApplicationError::ReceiptBudget)?;
        out[..8].copy_from_slice(&receipt.index.to_le_bytes());
        out[8..24].copy_from_slice(&receipt.operation.get().to_le_bytes());
        let (tag, value) = match receipt.outcome {
            CounterOutcome::Value(v) => (1, v),
            CounterOutcome::Overflow => (2, 0),
            CounterOutcome::OperationConflict => (3, 0),
            CounterOutcome::Registered(session) => (4, session as i64),
            CounterOutcome::Refused(refusal) => (5, refusal as i64),
        };
        out[24] = tag;
        out[25..33].copy_from_slice(&value.to_le_bytes());
        out[33] = u8::from(receipt.duplicate);
        Ok(34)
    }
    fn encode_query(&self, _: &(), _: &mut [u8]) -> Result<usize, ApplicationError> {
        Ok(0)
    }
    fn encode_read(&self, value: &i64, out: &mut [u8]) -> Result<usize, ApplicationError> {
        out.get_mut(..8)
            .ok_or(ApplicationError::ReceiptBudget)?
            .copy_from_slice(&value.to_le_bytes());
        Ok(if self.2.get() { 9 } else { 8 })
    }
}
