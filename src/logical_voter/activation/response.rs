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
use codec::Encoder;
use std::mem::size_of;
const RECEIPT_BYTES: usize = 59;
const STATUS_BYTES: usize = CONTROL_BYTES + 3 * RECEIPT_BYTES + 32;
fn publish(bytes: &[u8], out: &mut [u8]) -> Result<usize, ApplicationError> {
    if bytes.len() > out.len() {
        return Err(ApplicationError::ReceiptBudget);
    }
    out[..bytes.len()].copy_from_slice(bytes);
    Ok(bytes.len())
}
pub(super) fn receipt(e: &mut Encoder, r: ChildActivationReceipt) -> Result<(), ApplicationError> {
    e.u64(r.index)?;
    e.u128(r.operation.get())?;
    e.position(r.first)?;
    let (tag, busy) = match r.outcome {
        ChildActivationOutcome::Begun => (0, 0),
        ChildActivationOutcome::Busy(id) => (1, id.get()),
        ChildActivationOutcome::Completed => (2, 0),
        ChildActivationOutcome::Released => (3, 0),
        ChildActivationOutcome::OperationConflict => (4, 0),
    };
    e.u8(tag)?;
    e.u128(busy)?;
    e.u8(u8::from(r.duplicate))
}
fn status(e: &mut Encoder, s: &ChildActivationStatus) -> Result<(), ApplicationError> {
    codec::intent(e, &s.intent)?;
    receipt(e, s.begin)?;
    for r in [s.outcome, s.release] {
        e.u8(u8::from(r.is_some()))?;
        if let Some(r) = r {
            receipt(e, r)?;
        }
    }
    e.u8(u8::from(s.child_final.is_some()))?;
    if let Some(p) = s.child_final {
        e.position(p)?;
    }
    Ok(())
}
impl<A: ProposalAdmission + CheckpointStateMachine + BoundedReadableStateMachine>
    ReadableStateMachine for LogicalActivationApplication<A>
where
    A::Receipt: ApplicationReceipt,
{
    type Query = ActivationQuery<A::Query>;
    type ReadResult = ActivationRead<A::ReadResult>;
    fn read_at(
        &self,
        required: u64,
        query: Self::Query,
    ) -> Result<Self::ReadResult, ApplicationError> {
        if required > self.applied_index() {
            return Err(ApplicationError::NotApplied);
        }
        Ok(match query {
            ActivationQuery::Business(q) => {
                ActivationRead::Business(self.inner.read_at(required, q)?)
            }
            ActivationQuery::Status(id) => ActivationRead::Status(self.status(id).map(Box::new)),
            ActivationQuery::Active => ActivationRead::Active(self.active()),
        })
    }
}
impl<A: ProposalAdmission + CheckpointStateMachine + BoundedReadableStateMachine>
    BoundedReadableStateMachine for LogicalActivationApplication<A>
where
    A::Receipt: ApplicationReceipt,
{
    fn query_bytes(&self, q: &Self::Query, limit: usize) -> Result<usize, ApplicationError> {
        match q {
            ActivationQuery::Business(q) => self.inner.query_bytes(q, limit),
            _ => Ok(0),
        }
    }
    fn read_result_bound(&self, q: &Self::Query) -> Result<usize, ApplicationError> {
        let nested = match q {
            ActivationQuery::Business(q) => self.inner.read_result_bound(q)?,
            ActivationQuery::Status(_) => size_of::<ChildActivationStatus>(),
            ActivationQuery::Active => 0,
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
        let n = match r {
            ActivationRead::Business(r) => self.inner.read_result_bytes(r, limit)?,
            ActivationRead::Status(Some(_)) => size_of::<ChildActivationStatus>(),
            _ => 0,
        };
        if n > limit {
            return Err(ApplicationError::ReceiptBudget);
        }
        Ok(n)
    }
}
impl<A: ProposalAdmission + CheckpointStateMachine + LogicalResponseCodec> LogicalResponseCodec
    for LogicalActivationApplication<A>
where
    A::Receipt: ApplicationReceipt,
{
    fn logical_response_schema(&self) -> u64 {
        LOGICAL_ACTIVATION_RESPONSE_SCHEMA
    }
    fn logical_activation_control_profile(&self) -> Option<LogicalActivationControlProfile> {
        Some(self.control_profile())
    }
    fn receipt_encoding_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        self.inner
            .receipt_encoding_bound(&self.inner_entries(entries)?)?
            .max(RECEIPT_BYTES)
            .checked_add(1)
            .ok_or(ApplicationError::ReceiptBudget)
    }
    fn query_encoding_bound(&self, q: &Self::Query) -> Result<usize, ApplicationError> {
        match q {
            ActivationQuery::Business(q) => self
                .inner
                .query_encoding_bound(q)?
                .checked_add(1)
                .ok_or(ApplicationError::ReceiptBudget),
            ActivationQuery::Status(_) => Ok(17),
            ActivationQuery::Active => Ok(1),
        }
    }
    fn read_encoding_bound(&self, q: &Self::Query) -> Result<usize, ApplicationError> {
        match q {
            ActivationQuery::Business(q) => self
                .inner
                .read_encoding_bound(q)?
                .checked_add(1)
                .ok_or(ApplicationError::ReceiptBudget),
            ActivationQuery::Status(_) => Ok(STATUS_BYTES + 2),
            ActivationQuery::Active => Ok(17),
        }
    }
    fn encode_receipt(&self, r: &Self::Receipt, out: &mut [u8]) -> Result<usize, ApplicationError> {
        let Some((tag, rest)) = out.split_first_mut() else {
            return Err(ApplicationError::ReceiptBudget);
        };
        match r {
            ActivationReceipt::Business(r) => {
                *tag = 0;
                self.inner
                    .encode_receipt(r, rest)?
                    .checked_add(1)
                    .ok_or(ApplicationError::ReceiptBudget)
            }
            ActivationReceipt::Control(r) => {
                *tag = 1;
                let mut e = Encoder::new(RECEIPT_BYTES);
                receipt(&mut e, *r)?;
                Ok(1 + publish(&e.bytes, rest)?)
            }
        }
    }
    fn encode_query(&self, q: &Self::Query, out: &mut [u8]) -> Result<usize, ApplicationError> {
        if let ActivationQuery::Business(q) = q {
            let Some((tag, rest)) = out.split_first_mut() else {
                return Err(ApplicationError::ReceiptBudget);
            };
            *tag = 0;
            return self
                .inner
                .encode_query(q, rest)?
                .checked_add(1)
                .ok_or(ApplicationError::ReceiptBudget);
        }
        let mut e = Encoder::new(17);
        match q {
            ActivationQuery::Status(id) => {
                e.u8(1)?;
                e.u128(id.get())?;
            }
            ActivationQuery::Active => e.u8(2)?,
            _ => unreachable!(),
        }
        publish(&e.bytes, out)
    }
    fn encode_read(&self, r: &Self::ReadResult, out: &mut [u8]) -> Result<usize, ApplicationError> {
        if let ActivationRead::Business(r) = r {
            let Some((tag, rest)) = out.split_first_mut() else {
                return Err(ApplicationError::ReceiptBudget);
            };
            *tag = 0;
            return self
                .inner
                .encode_read(r, rest)?
                .checked_add(1)
                .ok_or(ApplicationError::ReceiptBudget);
        }
        let mut e = Encoder::new(STATUS_BYTES + 2);
        match r {
            ActivationRead::Status(s) => {
                e.u8(1)?;
                e.u8(u8::from(s.is_some()))?;
                if let Some(s) = s {
                    status(&mut e, s)?;
                }
            }
            ActivationRead::Active(id) => {
                e.u8(2)?;
                e.u128(id.map_or(0, OperationId::get))?;
            }
            _ => unreachable!(),
        }
        publish(&e.bytes, out)
    }
}
