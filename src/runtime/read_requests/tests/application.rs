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
//! Faithful host query codec for selected wrapper classification controls.
use super::*;
use crate::application::*;
use crate::log::LogEntry;
#[derive(Clone)]
pub(super) struct App(Counter, std::cell::Cell<usize>);
impl App {
    pub(super) fn new() -> Self {
        Self(Counter::new(16).unwrap(), std::cell::Cell::new(0))
    }
    pub(super) fn query_encodes(&self) -> usize {
        self.1.get()
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
}
impl BoundedStateMachine for App {
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        self.0.receipt_bytes_bound(entries)
    }
}
impl ProposalAdmission for App {
    fn validate_proposal<'a>(
        &self,
        op: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError> {
        self.0.validate_proposal(op, bytes, pending)
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
impl ReadableStateMachine for App {
    type Query = Vec<u8>;
    type ReadResult = i64;
    fn read_at(&self, index: u64, _: Vec<u8>) -> Result<i64, ApplicationError> {
        self.0.read_applied(index)
    }
}
impl BoundedReadableStateMachine for App {
    fn query_bytes(&self, query: &Vec<u8>, limit: usize) -> Result<usize, ApplicationError> {
        if query.capacity() > limit {
            Err(ApplicationError::ReceiptBudget)
        } else {
            Ok(query.capacity())
        }
    }
    fn read_result_bound(&self, _: &Vec<u8>) -> Result<usize, ApplicationError> {
        Ok(size_of::<i64>())
    }
    fn read_result_bytes(&self, _: &i64, _: usize) -> Result<usize, ApplicationError> {
        Ok(0)
    }
}
impl LogicalResponseCodec for App {
    fn logical_response_schema(&self) -> u64 {
        1
    }
    fn receipt_encoding_bound(&self, _: &[LogEntry]) -> Result<usize, ApplicationError> {
        Ok(34)
    }
    fn query_encoding_bound(&self, q: &Vec<u8>) -> Result<usize, ApplicationError> {
        Ok(q.len())
    }
    fn read_encoding_bound(&self, _: &Vec<u8>) -> Result<usize, ApplicationError> {
        Ok(8)
    }
    fn encode_receipt(
        &self,
        r: &CounterReceipt,
        out: &mut [u8],
    ) -> Result<usize, ApplicationError> {
        let out = out.get_mut(..34).ok_or(ApplicationError::ReceiptBudget)?;
        out[..8].copy_from_slice(&r.index.to_le_bytes());
        out[8..24].copy_from_slice(&r.operation.get().to_le_bytes());
        let (tag, value) = match r.outcome {
            CounterOutcome::Value(v) => (1, v),
            CounterOutcome::Overflow => (2, 0),
            CounterOutcome::OperationConflict => (3, 0),
            CounterOutcome::Registered(session) => (4, session as i64),
            CounterOutcome::Refused(refusal) => (5, refusal as i64),
        };
        out[24] = tag;
        out[25..33].copy_from_slice(&value.to_le_bytes());
        out[33] = u8::from(r.duplicate);
        Ok(34)
    }
    fn encode_query(&self, q: &Vec<u8>, out: &mut [u8]) -> Result<usize, ApplicationError> {
        self.1.set(self.1.get() + 1);
        out.get_mut(..q.len())
            .ok_or(ApplicationError::ReceiptBudget)?
            .copy_from_slice(q);
        Ok(q.len())
    }
    fn encode_read(&self, value: &i64, out: &mut [u8]) -> Result<usize, ApplicationError> {
        out.get_mut(..8)
            .ok_or(ApplicationError::ReceiptBudget)?
            .copy_from_slice(&value.to_le_bytes());
        Ok(8)
    }
}
