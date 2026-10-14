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
use super::*;
/// Canonical padded Counter command for exercising provider capacity, not a
/// readiness certificate or a different consensus protocol.
#[derive(Clone)]
pub(super) struct LargeApplication(pub(super) HostApplication);
fn canonical(bytes: &[u8]) -> Result<&[u8], ApplicationError> {
    if ![COMMAND_BYTES, DEFAULT_REFUSAL_BYTES].contains(&bytes.len())
        || bytes[8..].iter().any(|b| *b != 0)
    {
        return Err(ApplicationError::InvalidCommand);
    }
    Ok(&bytes[..8])
}
fn entries(input: &[LogEntry]) -> Result<Vec<LogEntry>, ApplicationError> {
    input
        .iter()
        .map(|original| {
            let mut entry = original.clone();
            if let EntryPayload::Command { bytes, .. } = &mut entry.payload {
                *bytes = canonical(bytes)?.into();
            }
            Ok(entry)
        })
        .collect()
}
impl StateMachine for LargeApplication {
    type Receipt = CounterReceipt;
    fn applied_index(&self) -> u64 {
        self.0.applied_index()
    }
    fn apply_batch(&mut self, input: &[LogEntry]) -> Result<Vec<CounterReceipt>, ApplicationError> {
        self.0.apply_batch(&entries(input)?)
    }
}
impl BoundedStateMachine for LargeApplication {
    fn receipt_bytes_bound(&self, input: &[LogEntry]) -> Result<usize, ApplicationError> {
        self.0.receipt_bytes_bound(&entries(input)?)
    }
}
impl ProposalAdmission for LargeApplication {
    fn validate_proposal<'a>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError> {
        let bytes = canonical(bytes)?;
        let pending = pending
            .map(|(o, b)| canonical(b).map(|b| (o, b)))
            .collect::<Result<Vec<_>, _>>()?;
        self.0
            .validate_proposal(operation, bytes, pending.into_iter())
    }
}
impl ReadableStateMachine for LargeApplication {
    type Query = ();
    type ReadResult = i64;
    fn read_at(&self, index: u64, query: ()) -> Result<i64, ApplicationError> {
        self.0.read_at(index, query)
    }
}
impl BoundedReadableStateMachine for LargeApplication {
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
impl CheckpointStateMachine for LargeApplication {
    fn schema_version(&self) -> u64 {
        self.0.schema_version()
    }
    fn checkpoint(&self, limit: usize) -> Result<Vec<u8>, ApplicationError> {
        self.0.checkpoint(limit)
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
