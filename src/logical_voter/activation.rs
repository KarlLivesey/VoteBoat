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
//! Opt-in parent-replicated child replacement slot. Internal phase bytes are
//! trusted protocol-driver assertions; neither status nor receipts mint authority.
use super::LogicalStoreMapping;
use crate::{application::*, identity::*, log::*, runtime::LogicalResponseCodec};
use std::collections::{BTreeMap, BTreeSet};
mod driver;
pub use driver::*;
mod types;
pub use types::*;
mod admission;
mod checkpoint;
mod codec;
mod control_profile;
pub(crate) use control_profile::LogicalActivationCommandClass;
pub use control_profile::LogicalActivationControlProfile;
mod response;
mod state;
#[cfg(test)]
mod tests;
mod traits;
use state::{ControlCommand, ControlState};
/// The activation wrapper's format identifier. Its application schema is
/// [`logical_activation_schema`] of the inner schema, not this constant.
pub const LOGICAL_ACTIVATION_SCHEMA: u64 = 0x5642_4143_5449_0002;
/// The application schema of an activation wrapper around an application with
/// schema `inner`: [`layered_schema`]`(`[`LOGICAL_ACTIVATION_LAYER`]`, inner)`.
/// `None` for an inner schema of zero or at or above 2^60, which includes
/// every composed (activation or maintenance) schema, so wrappers do not
/// nest. Within that domain the map is injective and disjoint from plain and
/// maintenance schemas: wrapped replicas whose inner applications differ in
/// identity (for a Counter, its session limits) refuse each other at
/// readiness, snapshot install and restore.
pub const fn logical_activation_schema(inner: u64) -> Option<u64> {
    layered_schema(LOGICAL_ACTIVATION_LAYER, inner)
}
pub const LOGICAL_ACTIVATION_RESPONSE_SCHEMA: u64 = 0x5642_4143_5452_0001;
const CONTROL_BYTES: usize = 480;
const BUSINESS_HEADER: usize = 9;

#[derive(Clone)]
pub struct LogicalActivationApplication<A> {
    parent: GroupIdentity,
    mappings: Vec<LogicalStoreMapping>,
    limits: LogicalActivationLimits,
    inner: A,
    /// [`logical_activation_schema`] of the inner schema, fixed at construction.
    schema: u64,
    control: ControlState,
}
impl<A: ProposalAdmission + CheckpointStateMachine> LogicalActivationApplication<A>
where
    A::Receipt: ApplicationReceipt,
{
    pub fn new(
        parent: GroupIdentity,
        mut mappings: Vec<LogicalStoreMapping>,
        limits: LogicalActivationLimits,
        inner: A,
    ) -> Result<Self, ApplicationError> {
        let limits = limits.validate()?;
        inner.validate_group(parent)?;
        if inner.applied_index() != 0 || mappings.is_empty() || mappings.len() > 16 {
            return Err(ApplicationError::InvalidCommand);
        }
        // Refuses a nested wrapper and any inner schema outside the domain.
        let schema = logical_activation_schema(inner.schema_version())
            .ok_or(ApplicationError::UnsupportedSchema)?;
        mappings.sort_by_key(|m| m.voter);
        for (i, m) in mappings.iter().enumerate() {
            if m.parent != parent
                || LogicalStoreMapping::new(m.parent, m.voter, m.store, m.child)? != *m
                || mappings[..i]
                    .iter()
                    .any(|old| old.voter == m.voter || old.child == m.child || old.store == m.store)
            {
                return Err(ApplicationError::InvalidCommand);
            }
        }
        let a = Self {
            parent,
            mappings,
            limits,
            inner,
            schema,
            control: ControlState::default(),
        };
        a.checkpoint(limits.checkpoint_bytes)?;
        Ok(a)
    }
    pub fn parent(&self) -> GroupIdentity {
        self.parent
    }
    pub fn inner(&self) -> &A {
        &self.inner
    }
    pub fn limits(&self) -> LogicalActivationLimits {
        self.limits
    }
    pub fn mappings(&self) -> &[LogicalStoreMapping] {
        &self.mappings
    }
    /// Original business operation and bytes are passed unchanged to the inner app.
    pub fn business(&self, bytes: &[u8]) -> Result<Vec<u8>, ApplicationError> {
        let mut e = codec::Encoder::new(self.limits.command_bytes);
        e.put(codec::COMMAND)?;
        e.u8(0)?;
        e.put(bytes)?;
        Ok(e.bytes)
    }
    pub fn begin(&self, intent: &ChildActivationIntent) -> Result<Vec<u8>, ApplicationError> {
        self.check_intent(intent)?;
        codec::command(
            &ControlCommand::Begin(Box::new(intent.clone())),
            self.limits.command_bytes,
        )
    }
    /// Applied-local inspection only. Distributed authority requires fresh Node reads.
    pub fn status(&self, begin: OperationId) -> Option<ChildActivationStatus> {
        self.control.records.get(&begin).map(|r| r.status())
    }
    pub fn active(&self) -> Option<OperationId> {
        self.control.active
    }
    fn check_intent(&self, intent: &ChildActivationIntent) -> Result<(), ApplicationError> {
        intent.validate()?;
        if intent.mapping.parent != self.parent || !self.mappings.contains(&intent.mapping) {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(())
    }
}
