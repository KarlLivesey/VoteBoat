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
//! Adoption of an original local-reparenting quorum observation by its data owner.
use super::parent_slots::{CrossParentSlotAdoption, ParentSlotAdoption};
use super::*;
use crate::reparenting::{ReparentPlan, ReparentStatus, MAX_REPARENT_PLAN_BYTES};
pub const PARENT_ADOPTING_ROUTED_SCHEMA: u64 = 3;
pub const MAX_PARENT_ADOPTIONS: usize = 64;
pub const MAX_PARENT_ADOPTION_BYTES: usize = 44 + MAX_REPARENT_PLAN_BYTES;
/// Host-authenticated original metadata quorum observation. This constructible
/// value and its encoding are not a commitment/authorization certificate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnerParentAdoption {
    pub metadata_configuration: ConfigurationId,
    pub decision: ReparentStatus,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParentGrantStatus {
    pub operation: OperationId,
    pub index: u64,
    pub metadata_operation: OperationId,
    pub metadata_index: u64,
    pub generation: RouteGeneration,
}
#[derive(Clone)]
pub(super) struct ParentAdoptionRecord {
    pub status: ParentGrantStatus,
    pub command: ParentAdoptionCommand,
}
#[derive(Clone, Debug, Eq, PartialEq)]
// Keep the original local command inline: at most 64 records, with no new
// per-record allocation or failure path. Both variants' nested data are bounded.
#[allow(clippy::large_enum_variant)]
pub(crate) enum ParentAdoptionCommand {
    Local(OwnerParentAdoption),
    Cross(CrossOwnerParentAdoption),
    LocalSlots(ParentSlotAdoption),
    CrossSlots(CrossParentSlotAdoption),
    Metadata(OwnerMetadataAdoption),
}
impl ParentAdoptionCommand {
    pub fn decode_scoped(bytes: &[u8], slots: bool) -> Result<Self, ApplicationError> {
        if slots && bytes.starts_with(b"VBSLAD01") {
            ParentSlotAdoption::decode(bytes).map(Self::LocalSlots)
        } else if slots && bytes.starts_with(b"VBSXAD01") {
            CrossParentSlotAdoption::decode(bytes).map(Self::CrossSlots)
        } else {
            Self::decode(bytes, true)
        }
    }
    pub fn decode(bytes: &[u8], cross: bool) -> Result<Self, ApplicationError> {
        if cross && bytes.starts_with(b"VBXPAD01") {
            CrossOwnerParentAdoption::decode(bytes).map(Self::Cross)
        } else {
            OwnerParentAdoption::decode(bytes).map(Self::Local)
        }
    }
    pub fn decode_metadata(
        bytes: &[u8],
        cross: bool,
        metadata: bool,
    ) -> Result<Self, ApplicationError> {
        if metadata && bytes.starts_with(b"VBMAAD01") {
            OwnerMetadataAdoption::decode(bytes).map(Self::Metadata)
        } else {
            Self::decode(bytes, cross)
        }
    }
    pub fn encode(&self, max: usize) -> Result<Vec<u8>, ApplicationError> {
        match self {
            Self::Local(a) => a.encode(max),
            Self::Cross(a) => a.encode(max),
            Self::LocalSlots(a) => a.encode(max),
            Self::CrossSlots(a) => a.encode(max),
            Self::Metadata(a) => a.encode(max),
        }
    }
    pub fn before(&self) -> &ResponsibilityManifest {
        match self {
            Self::Local(a) => a.before(),
            Self::Cross(a) => a.before(),
            Self::LocalSlots(a) => a.before(),
            Self::CrossSlots(a) => a.before(),
            Self::Metadata(a) => a.before(),
        }
    }
    pub fn after(&self) -> ResponsibilityManifest {
        match self {
            Self::Local(a) => a.after(),
            Self::Cross(a) => a.after(),
            Self::LocalSlots(a) => a.after(),
            Self::CrossSlots(a) => a.after(),
            Self::Metadata(a) => a.after(),
        }
    }
    pub fn metadata(&self) -> (OperationId, u64) {
        match self {
            Self::Local(a) => (a.decision.operation, a.decision.index),
            Self::Cross(a) => (a.child_publication.operation, a.child_publication.index),
            Self::LocalSlots(a) => (
                a.observation.decision.operation,
                a.observation.decision.index,
            ),
            Self::CrossSlots(a) => (a.parent_publication.operation, a.parent_publication.index),
            // The metadata operation is the original transfer ID, but this index
            // belongs to the new authority's activation, not the old source F.
            Self::Metadata(a) => (
                a.activation().publication.imported.source.operation,
                a.activation().index,
            ),
        }
    }
}
impl OwnerParentAdoption {
    pub fn encode(&self, max: usize) -> Result<Vec<u8>, ApplicationError> {
        if self.decision.index == 0 || self.decision.index == u64::MAX {
            return Err(ApplicationError::InvalidCommand);
        }
        let plan = self.decision.plan.encode(MAX_REPARENT_PLAN_BYTES)?;
        if 44 + plan.len() > max.min(MAX_PARENT_ADOPTION_BYTES) {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut b = Vec::with_capacity(44 + plan.len());
        b.extend(b"VBRPAD01");
        b.extend(self.metadata_configuration.get().to_le_bytes());
        b.extend(self.decision.operation.get().to_le_bytes());
        b.extend(self.decision.index.to_le_bytes());
        b.extend((plan.len() as u32).to_le_bytes());
        b.extend(plan);
        Ok(b)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, ApplicationError> {
        if bytes.len() > MAX_PARENT_ADOPTION_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBRPAD01" {
            return Err(ApplicationError::InvalidCommand);
        }
        let metadata_configuration =
            ConfigurationId::new(r.u64()?).ok_or(ApplicationError::InvalidCommand)?;
        let operation = r.operation()?;
        let index = r.u64()?;
        let n = r.u32()? as usize;
        let plan = ReparentPlan::decode(r.take(n)?)?;
        if !r.done() || index == 0 || index == u64::MAX {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(Self {
            metadata_configuration,
            decision: ReparentStatus {
                operation,
                index,
                plan,
            },
        })
    }
    pub fn before(&self) -> &ResponsibilityManifest {
        self.decision.plan.child()
    }
    pub fn after(&self) -> ResponsibilityManifest {
        self.decision
            .plan
            .updated_manifests()
            .into_iter()
            .nth(2)
            .unwrap()
    }
}
impl<A: CheckpointStateMachine, P: PartitionPolicy + Clone> RoutedApplication<A, P> {
    /// Select before bootstrap. Scoped retained-source profiles use a separate
    /// grant ledger and are deliberately not enabled by this selection.
    #[allow(clippy::result_large_err)]
    pub fn with_parent_adoption(
        mut self,
        maximum: usize,
    ) -> Result<Self, (ApplicationError, Self)> {
        if maximum == 0
            || maximum > MAX_PARENT_ADOPTIONS
            || self.parent_adoption_limit != 0
            || self.scope_limit != 0
            || self.inner.applied_index() != 0
            || self.initialized.is_some()
            || self.fence.is_some()
            || self.binding.len() + 2 > MAX_ROUTED_COMMAND_BYTES
        {
            return Err((ApplicationError::InvalidCommand, self));
        }
        self.parent_adoption_limit = maximum;
        self.binding[..8].copy_from_slice(b"VBROWN03");
        self.binding.extend((maximum as u16).to_le_bytes());
        Ok(self)
    }
    pub fn parent_adoption_limit(&self) -> usize {
        self.parent_adoption_limit
    }
    /// Select the larger, bounded cross-authority observation profile before
    /// bootstrap. It also accepts local adoptions in the same ordered ledger.
    #[allow(clippy::result_large_err)]
    pub fn with_cross_authority_parent_adoption(
        self,
        maximum: usize,
    ) -> Result<Self, (ApplicationError, Self)> {
        let mut selected = self.with_parent_adoption(maximum)?;
        selected.cross_parent_adoption = true;
        selected.binding[..8].copy_from_slice(b"VBROWN04");
        Ok(selected)
    }
    pub(super) fn parent_command_bound(&self) -> usize {
        if self.metadata_adoption {
            MAX_METADATA_ADOPTION_BYTES
        } else if self.cross_parent_adoption {
            MAX_CROSS_PARENT_ADOPTION_BYTES
        } else {
            MAX_PARENT_ADOPTION_BYTES
        }
    }
    /// Local applied diagnostic; foreign use requires the original owner quorum.
    pub fn parent_adoption(&self, operation: OperationId) -> Option<ParentGrantStatus> {
        self.parent_adoptions
            .iter()
            .find(|a| a.status.operation == operation)
            .map(|a| a.status)
    }
    pub(crate) fn parent_adoption_retry(
        &self,
        operation: OperationId,
        bytes: &[u8],
    ) -> Option<ParentGrantStatus> {
        let command = ParentAdoptionCommand::decode_metadata(
            bytes,
            self.cross_parent_adoption,
            self.metadata_adoption,
        )
        .ok()?;
        self.parent_adoptions
            .iter()
            .find(|a| a.status.operation == operation && a.command == command)
            .map(|a| a.status)
    }
    pub(super) fn parent_operation(&self, operation: OperationId) -> bool {
        self.parent_adoption(operation).is_some()
    }
    pub(super) fn apply_parent_adoption<R>(
        &mut self,
        operation: OperationId,
        index: u64,
        bytes: &[u8],
    ) -> Result<RoutedOutcome<R>, ApplicationError> {
        let command = ParentAdoptionCommand::decode_metadata(
            bytes,
            self.cross_parent_adoption,
            self.metadata_adoption,
        )?;
        if let Some(old) = self
            .parent_adoptions
            .iter()
            .find(|a| a.status.operation == operation)
        {
            return Ok(if old.command == command {
                RoutedOutcome::ParentAdopted(old.status)
            } else {
                RoutedOutcome::OperationConflict
            });
        }
        if self.initialized.is_some_and(|(op, _)| op == operation)
            || self.history.contains_key(&operation)
            || self.scoped_operation(operation)
            || self.fence.is_some_and(|f| f.operation == operation)
        {
            return Ok(RoutedOutcome::OperationConflict);
        }
        if self.fence.is_some() {
            return Ok(RoutedOutcome::Rejected(RoutingError::Fenced));
        }
        if self.grant != *command.before() {
            return Ok(RoutedOutcome::Rejected(RoutingError::WrongOwner));
        }
        if self.parent_adoptions.len() == self.parent_adoption_limit {
            return Err(ApplicationError::ReceiptBudget);
        }
        let after = command.after();
        let status = ParentGrantStatus {
            operation,
            index,
            metadata_operation: command.metadata().0,
            metadata_index: command.metadata().1,
            generation: after.input().generation,
        };
        self.parent_adoptions
            .try_reserve_exact(1)
            .map_err(|_| ApplicationError::ReceiptBudget)?;
        if self.parent_adoptions.capacity() > MAX_PARENT_ADOPTIONS {
            return Err(ApplicationError::ReceiptBudget);
        }
        self.parent_adoptions
            .push(ParentAdoptionRecord { status, command });
        self.grant = after;
        Ok(RoutedOutcome::ParentAdopted(status))
    }
    pub(super) fn bootstrap_grant(&self) -> Result<ResponsibilityManifest, ApplicationError> {
        let mut r = Reader::new(&self.binding);
        // Exact original immutable binding, including every configured limit.
        r.take(8)?;
        r.group()?;
        r.u32()?;
        r.u64()?;
        r.u32()?;
        r.u32()?;
        let n = r.u32()? as usize;
        read_manifest(r.take(n)?)
    }
}
