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
//! Immutable selected budget classification; this profile grants no protocol authority.
use super::*;
use codec::{Encoder, Envelope};
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalActivationControlProfile {
    parent: GroupIdentity,
    mappings: Vec<LogicalStoreMapping>,
    limits: LogicalActivationLimits,
    inner_schema: u64,
    application_schema: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LogicalActivationCommandClass {
    Business,
    Control,
}
impl<A: ProposalAdmission + CheckpointStateMachine> LogicalActivationApplication<A>
where
    A::Receipt: ApplicationReceipt,
{
    /// Inspect the actual immutable wrapper configuration. This selects budget
    /// classification only; it cannot create a consumed observation or permit.
    pub fn control_profile(&self) -> LogicalActivationControlProfile {
        LogicalActivationControlProfile {
            parent: self.parent,
            mappings: self.mappings.clone(),
            limits: self.limits,
            inner_schema: self.inner.schema_version(),
            application_schema: self.schema,
        }
    }
}
impl LogicalActivationControlProfile {
    pub fn parent(&self) -> GroupIdentity {
        self.parent
    }
    pub fn mappings(&self) -> &[LogicalStoreMapping] {
        &self.mappings
    }
    pub fn limits(&self) -> LogicalActivationLimits {
        self.limits
    }
    pub fn inner_schema(&self) -> u64 {
        self.inner_schema
    }
    pub fn application_schema(&self) -> u64 {
        self.application_schema
    }
    pub fn response_schema(&self) -> u64 {
        LOGICAL_ACTIVATION_RESPONSE_SCHEMA
    }
    pub fn format_version(&self) -> u32 {
        2
    }
    pub(crate) fn configuration_bytes(&self, max: usize) -> Result<Vec<u8>, ApplicationError> {
        let mut e = Encoder::new(max);
        e.put(b"VBACTP02")?;
        e.u64(u64::from(self.format_version()))?;
        e.group(self.parent)?;
        e.u64(self.mappings.len() as u64)?;
        for m in &self.mappings {
            e.mapping(*m)?;
        }
        for n in [
            self.limits.intents,
            self.limits.command_bytes,
            self.limits.checkpoint_bytes,
        ] {
            e.u64(n as u64)?;
        }
        for schema in [
            self.inner_schema,
            self.application_schema(),
            self.response_schema(),
        ] {
            e.u64(schema)?;
        }
        Ok(e.bytes)
    }
    pub(crate) fn classify_command(
        &self,
        operation: OperationId,
        bytes: &[u8],
    ) -> Result<LogicalActivationCommandClass, ApplicationError> {
        let Envelope::Control(c) = codec::decode(bytes, self.limits.command_bytes)? else {
            return Ok(LogicalActivationCommandClass::Business);
        };
        match c {
            ControlCommand::Begin(i) => {
                i.validate()?;
                if i.mapping.parent != self.parent
                    || !self.mappings.contains(&i.mapping)
                    || operation != i.operations.begin
                {
                    return Err(ApplicationError::InvalidCommand);
                }
            }
            ControlCommand::Outcome { begin, fact } => {
                if operation == begin
                    || operation == fact.operation
                    || begin == fact.operation
                    || fact.position.index == 0
                    || fact.position.term == 0
                {
                    return Err(ApplicationError::InvalidCommand);
                }
            }
            ControlCommand::Release { begin, outcome } => {
                if operation == begin || outcome.index == 0 || outcome.term == 0 {
                    return Err(ApplicationError::InvalidCommand);
                }
            }
        }
        Ok(LogicalActivationCommandClass::Control)
    }
    pub(crate) fn is_control_query(&self, bytes: &[u8]) -> bool {
        match bytes {
            [2] => true,
            [1, id @ ..] if id.len() == 16 => {
                let mut value = [0; 16];
                value.copy_from_slice(id);
                OperationId::new(u128::from_le_bytes(value)).is_some()
            }
            _ => false,
        }
    }
    /// Classify the complete original batch; never split an existing lease or
    /// grant protocol authority from its contents.
    pub(crate) fn is_control_batch(&self, group: GroupIdentity, entries: &[LogEntry]) -> bool {
        let mut commands = false;
        let mut control = false;
        for entry in entries {
            if let EntryPayload::Command { operation, bytes } = &entry.payload {
                commands = true;
                if group != self.parent {
                    return false;
                }
                match self.classify_command(*operation, bytes) {
                    Ok(LogicalActivationCommandClass::Control) => control = true,
                    Ok(LogicalActivationCommandClass::Business) => {}
                    Err(_) => return false,
                }
            }
        }
        !commands || control
    }
}
