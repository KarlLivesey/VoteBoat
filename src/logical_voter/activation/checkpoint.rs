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
use codec::{Decoder, Encoder};
const MAGIC: &[u8; 8] = b"VBACTC02";
impl<A: ProposalAdmission + CheckpointStateMachine> LogicalActivationApplication<A>
where
    A::Receipt: ApplicationReceipt,
{
    fn configuration_bytes(&self) -> Result<Vec<u8>, ApplicationError> {
        let mut e = Encoder::new(2048);
        e.group(self.parent)?;
        e.u64(self.inner.schema_version())?;
        for n in [
            self.limits.intents,
            self.limits.command_bytes,
            self.limits.checkpoint_bytes,
            self.mappings.len(),
        ] {
            e.u64(n as u64)?;
        }
        for m in &self.mappings {
            e.mapping(*m)?;
        }
        Ok(e.bytes)
    }
    fn materialized(&self, e: &mut Encoder) -> Result<(), ApplicationError> {
        e.u128(self.control.active.map_or(0, OperationId::get))?;
        e.u64(self.control.records.len() as u64)?;
        for (id, r) in &self.control.records {
            e.u128(id.get())?;
            response::receipt(e, r.begin.receipt)?;
            for phase in [&r.outcome, &r.release] {
                e.u8(u8::from(phase.is_some()))?;
                if let Some(p) = phase {
                    response::receipt(e, p.receipt)?;
                }
            }
        }
        Ok(())
    }
    fn checkpoint_bytes(&self, max: usize) -> Result<Vec<u8>, ApplicationError> {
        let mut e = Encoder::new(max.min(self.limits.checkpoint_bytes));
        e.put(MAGIC)?;
        e.blob(&self.configuration_bytes()?)?;
        e.u64(self.applied_index())?;
        let inner_max = self
            .limits
            .checkpoint_bytes
            .checked_sub(self.limits.control_checkpoint_bound()?)
            .ok_or(ApplicationError::ReceiptBudget)?;
        e.blob(&self.inner.checkpoint(inner_max)?)?;
        let phases = self.control.phases();
        e.u64(phases.len() as u64)?;
        for phase in phases {
            e.position(phase.receipt.first)?;
            e.blob(&codec::command(&phase.command, CONTROL_BYTES)?)?;
        }
        self.materialized(&mut e)?;
        Ok(e.bytes)
    }
    fn decode_checkpoint(&self, applied: u64, bytes: &[u8]) -> Result<Self, ApplicationError> {
        if bytes.len() > self.limits.checkpoint_bytes {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut d = Decoder::new(bytes);
        if d.take(8)? != MAGIC
            || d.blob(2048)? != self.configuration_bytes()?
            || d.u64()? != applied
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut next = self.clone();
        next.control = ControlState::default();
        next.inner.restore_checkpoint(
            self.inner.schema_version(),
            applied,
            d.blob(self.limits.checkpoint_bytes)?,
        )?;
        if next.inner.applied_index() != applied {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let count = self.count(&mut d, self.limits.intents * 3)?;
        let mut previous = 0;
        for _ in 0..count {
            let position = d.position()?;
            if position.index <= previous || position.index > applied || position.term == 0 {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            previous = position.index;
            let codec::Envelope::Control(c) = codec::decode(d.blob(CONTROL_BYTES)?, CONTROL_BYTES)?
            else {
                return Err(ApplicationError::InvalidCheckpoint);
            };
            let op = next.control.operation(&c)?;
            let receipt = next.execute_control(op, c, position.index, position.term)?;
            if receipt.duplicate
                || receipt.first != position
                || receipt.outcome == ChildActivationOutcome::OperationConflict
            {
                return Err(ApplicationError::InvalidCheckpoint);
            }
        }
        // Comparison includes original history, materialized receipts/slot and
        // inner state. Input cannot repair itself via getters.
        let expected = next.checkpoint_bytes(self.limits.checkpoint_bytes)?;
        if expected != bytes {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        Ok(next)
    }
    fn count(&self, d: &mut Decoder<'_>, max: usize) -> Result<usize, ApplicationError> {
        usize::try_from(d.u64()?)
            .ok()
            .filter(|n| *n <= max)
            .ok_or(ApplicationError::InvalidCheckpoint)
    }
}
impl<A: ProposalAdmission + CheckpointStateMachine> CheckpointStateMachine
    for LogicalActivationApplication<A>
where
    A::Receipt: ApplicationReceipt,
{
    fn schema_version(&self) -> u64 {
        self.schema
    }
    fn checkpoint(&self, max: usize) -> Result<Vec<u8>, ApplicationError> {
        self.checkpoint_bytes(max)
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
        let next = self.decode_checkpoint(applied, bytes).map_err(|e| {
            if e == ApplicationError::UnsupportedSchema {
                e
            } else {
                ApplicationError::InvalidCheckpoint
            }
        })?;
        *self = next;
        Ok(())
    }
}
