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
//! Faithful bounded schema for original logical application responses.
use super::*;
use crate::runtime::LogicalResponseCodec;
use codec::Encoder;
mod atoms;
mod configurations;
mod values;
/// Schema for exact typed logical receipts, queries and read values.
pub const LOGICAL_RESPONSE_SCHEMA: u64 = 0x5642_4c52_5350_0003;
pub const LOGICAL_ENROLLMENT_RESPONSE_SCHEMA: u64 = 0x5642_4c52_5350_0004;
/// Response format v3: `Expired`, the tri-state outcome lookup and the
/// metadata watermark.
const MAGIC: &[u8; 8] = b"VBLOGR03";
const CANDIDATE_MAGIC: &[u8; 8] = b"VBLOGR04";
const RECEIPT_BOUND: usize = 171;
const METADATA_BOUND: usize = 203;
const OUTCOME_OVERHEAD: usize = 221;

fn encoder(kind: u8, limit: usize, candidate: bool) -> Result<Encoder, ApplicationError> {
    let mut e = Encoder::new(limit);
    e.bytes = Vec::with_capacity(limit);
    if e.bytes.capacity() > limit {
        return Err(ApplicationError::ReceiptBudget);
    }
    e.put(if candidate { CANDIDATE_MAGIC } else { MAGIC })?;
    e.u8(kind)?;
    Ok(e)
}
fn publish(bytes: Vec<u8>, out: &mut [u8]) -> Result<usize, ApplicationError> {
    if bytes.len() > out.len() {
        return Err(ApplicationError::ReceiptBudget);
    }
    out[..bytes.len()].copy_from_slice(&bytes);
    Ok(bytes.len())
}
impl<C: LogCodec + Clone, S: SnapshotCodec + Clone, W: WireCodec + Clone> LogicalResponseCodec
    for LogicalApplication<C, S, W>
{
    fn logical_response_schema(&self) -> u64 {
        if self.candidate() {
            LOGICAL_ENROLLMENT_RESPONSE_SCHEMA
        } else {
            LOGICAL_RESPONSE_SCHEMA
        }
    }
    fn receipt_encoding_bound(&self, _: &[LogEntry]) -> Result<usize, ApplicationError> {
        Ok(RECEIPT_BOUND)
    }
    fn query_encoding_bound(&self, query: &LogicalQuery) -> Result<usize, ApplicationError> {
        Ok(match query {
            LogicalQuery::Image | LogicalQuery::Metadata => 10,
            LogicalQuery::Enrollment if self.candidate() => 10,
            LogicalQuery::Enrollment => return Err(ApplicationError::UnsupportedSchema),
            LogicalQuery::Outcome(_) | LogicalQuery::ConfigurationOperation(_) => 26,
            LogicalQuery::Snapshot(_) => 114,
        })
    }
    fn read_encoding_bound(&self, query: &LogicalQuery) -> Result<usize, ApplicationError> {
        match query {
            LogicalQuery::Image
            | LogicalQuery::Enrollment
            | LogicalQuery::Snapshot(_)
            | LogicalQuery::ConfigurationOperation(_) => {
                Ok(self.limits.max_parent_checkpoint_bytes)
            }
            LogicalQuery::Outcome(_) => self
                .limits
                .max_command_bytes
                .checked_add(OUTCOME_OVERHEAD)
                .ok_or(ApplicationError::ReceiptBudget),
            LogicalQuery::Metadata => Ok(METADATA_BOUND),
        }
    }
    fn encode_receipt(
        &self,
        receipt: &LogicalReceipt,
        out: &mut [u8],
    ) -> Result<usize, ApplicationError> {
        let mut e = encoder(0, out.len().min(RECEIPT_BOUND), self.candidate())?;
        atoms::receipt(&mut e, receipt)?;
        publish(e.bytes, out)
    }
    fn encode_query(
        &self,
        query: &LogicalQuery,
        out: &mut [u8],
    ) -> Result<usize, ApplicationError> {
        let mut e = encoder(
            1,
            out.len().min(self.query_encoding_bound(query)?),
            self.candidate(),
        )?;
        atoms::query(&mut e, query)?;
        publish(e.bytes, out)
    }
    fn encode_read(&self, value: &LogicalRead, out: &mut [u8]) -> Result<usize, ApplicationError> {
        publish(self.response_bytes(value, out.len())?, out)
    }
}
impl<C: LogCodec + Clone, S: SnapshotCodec + Clone, W: WireCodec + Clone>
    LogicalApplication<C, S, W>
{
    fn response_bytes(
        &self,
        value: &LogicalRead,
        limit: usize,
    ) -> Result<Vec<u8>, ApplicationError> {
        if matches!(
            value,
            LogicalRead::Image(_)
                | LogicalRead::Enrollment(_)
                | LogicalRead::Snapshot(_)
                | LogicalRead::ConfigurationOperation(_)
        ) {
            self.read_result_bytes(value, self.limits.max_parent_checkpoint_bytes)?;
        }
        let query = match value {
            LogicalRead::Image(_) | LogicalRead::Snapshot(_) | LogicalRead::Uninitialized => {
                LogicalQuery::Image
            }
            LogicalRead::Enrollment(_) => LogicalQuery::Enrollment,
            LogicalRead::Outcome(_) => LogicalQuery::Outcome(OperationId::new(1).unwrap()),
            LogicalRead::Metadata(_) => LogicalQuery::Metadata,
            LogicalRead::ConfigurationOperation(h) => {
                LogicalQuery::ConfigurationOperation(h.operation)
            }
        };
        let mut e = encoder(
            2,
            limit.min(self.read_encoding_bound(&query)?),
            self.candidate(),
        )?;
        self.response_value(&mut e, value)?;
        Ok(e.bytes)
    }
    pub(super) fn validate_response_initial(
        &self,
        parent: &GroupLog,
    ) -> Result<(), ApplicationError> {
        let (&node, &store) = self
            .child_bootstrap
            .voter_stores
            .first_key_value()
            .ok_or(ApplicationError::InvalidCommand)?;
        let representative = RepresentativeBinding {
            epoch: RepresentativeEpoch::new(1).unwrap(),
            physical: crate::secure::PeerIdentity { node, store },
            physical_session: StoreSession::new(1).unwrap(),
            logical_session: StoreSession::new(1).unwrap(),
        };
        let image = LogicalImage {
            mapping: self.mapping,
            representative,
            parent: parent.clone(),
            child_membership: self.state.membership.clone(),
            child_applied: self.state.applied,
            published: None,
            pins: Vec::new(),
        };
        self.response_bytes(
            &LogicalRead::Image(Box::new(image)),
            self.limits.max_parent_checkpoint_bytes,
        )?;
        Ok(())
    }
    pub(super) fn validate_response_envelopes(&self) -> Result<(), ApplicationError> {
        for query in [LogicalQuery::Image, LogicalQuery::Metadata] {
            let value = self.read_at(self.state.applied, query)?;
            self.response_bytes(&value, self.read_encoding_bound(&query)?)?;
        }
        if self.candidate() {
            let value = self.read_at(self.state.applied, LogicalQuery::Enrollment)?;
            self.read_result_bytes(&value, self.limits.max_parent_checkpoint_bytes)?;
            self.response_bytes(&value, self.limits.max_parent_checkpoint_bytes)?;
        }
        for (_, snapshot) in &self.state.snapshots {
            let mut e = encoder(2, self.limits.max_parent_checkpoint_bytes, self.candidate())?;
            e.u8(2)?;
            self.response_snapshot(&mut e, snapshot)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
