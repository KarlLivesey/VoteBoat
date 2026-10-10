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
//! Parent-only grant changes preserve every earlier scoped export.
use super::*;
use crate::routed::parent_adoption::ParentAdoptionCommand;
pub const PARENT_SCOPED_TRANSFER_SOURCE_SCHEMA: u64 = 4;
pub(super) fn parent_command(bytes: &[u8]) -> bool {
    bytes.starts_with(b"VBRPAD01")
        || bytes.starts_with(b"VBXPAD01")
        || bytes.starts_with(b"VBSLAD01")
        || bytes.starts_with(b"VBSXAD01")
}
impl<A, P> ScopedTransferSource<A, P>
where
    A: ScopeStateMachine + BoundedStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    pub(super) fn parent_command_bound(&self) -> usize {
        if self.parent_slots {
            MAX_PARENT_SLOT_ADOPTION_BYTES
        } else {
            MAX_CROSS_PARENT_ADOPTION_BYTES
        }
    }
    /// Select schema5 before bootstrap, after retained grants. The same bounded
    /// parent ledger accepts both moved-owner and parent child-slot observations.
    #[allow(clippy::result_large_err)]
    pub fn with_parent_slot_adoption(
        self,
        maximum: usize,
    ) -> Result<Self, (ApplicationError, Self)> {
        let mut next = self.with_parent_adoption(maximum)?;
        let extra = maximum * (MAX_PARENT_SLOT_ADOPTION_BYTES - MAX_CROSS_PARENT_ADOPTION_BYTES);
        if next
            .readiness_requirements()
            .snapshot_bytes
            .checked_add(extra)
            .is_none_or(|n| n > MAX_SCOPED_SOURCE_CHECKPOINT_BYTES)
        {
            next.parent_limit = 0;
            return Err((ApplicationError::InvalidCommand, next));
        }
        next.parent_slots = true;
        Ok(next)
    }
    /// Select before bootstrap, after retained grants. Supports authenticated
    /// local and completed cross-authority moves with independent lifetime reserve.
    #[allow(clippy::result_large_err)]
    pub fn with_parent_adoption(
        mut self,
        maximum: usize,
    ) -> Result<Self, (ApplicationError, Self)> {
        if maximum == 0 || maximum > MAX_PARENT_ADOPTIONS {
            return Err((ApplicationError::InvalidCommand, self));
        }
        let extra =
            2 + maximum * (61 + MAX_CROSS_PARENT_ADOPTION_BYTES) + self.routed.scoped_fence_limit();
        if self.parent_limit != 0
            || !self.retained_grants
            || self.applied_index() != 0
            || self
                .readiness_requirements()
                .snapshot_bytes
                .checked_add(extra)
                .is_none_or(|n| n > MAX_SCOPED_SOURCE_CHECKPOINT_BYTES)
        {
            return Err((ApplicationError::InvalidCommand, self));
        }
        self.parent_limit = maximum;
        Ok(self)
    }
    pub fn parent_adoption(&self, op: OperationId) -> Option<ParentGrantStatus> {
        self.adoptions.iter().find_map(|a| match a {
            GrantChange::Parent { status, .. } if status.operation == op => Some(*status),
            _ => None,
        })
    }
    pub(super) fn checked_parent(
        &self,
        op: OperationId,
        index: u64,
        command: ParentAdoptionCommand,
    ) -> Result<GrantChange, ApplicationError> {
        if self.parent_limit == 0 {
            return Err(ApplicationError::UnsupportedSchema);
        }
        if let Some(a) = self.adoptions.iter().find(|a| a.operation() == op) {
            return match a {
                GrantChange::Parent { command: old, .. } if *old == command => Ok(a.clone()),
                _ => Err(ApplicationError::InvalidCommand),
            };
        }
        let Some((boot, boot_index)) = self.routed.initialization() else {
            return Err(ApplicationError::NotApplied);
        };
        if index <= boot_index
            || self.routed.fence().is_some_and(|f| f.index <= index)
            || command.before() != self.grant()
            || boot == op
            || self.routed.has_data_operation(op)
            || self.routed.application().contains_operation(op)
            || self.reserved_creation(op)
            || self
                .routed
                .scoped_fences()
                .iter()
                .any(|f| f.fence.operation == op)
            || self
                .adoptions
                .iter()
                .filter(|a| matches!(a, GrantChange::Parent { .. }))
                .count()
                >= self.parent_limit
        {
            return Err(ApplicationError::InvalidCommand);
        }
        let (metadata_operation, metadata_index) = command.metadata();
        let status = ParentGrantStatus {
            operation: op,
            index,
            metadata_operation,
            metadata_index,
            generation: command.after().input().generation,
        };
        Ok(GrantChange::Parent { status, command })
    }
    pub(super) fn apply_parent(
        &mut self,
        op: OperationId,
        index: u64,
        bytes: &[u8],
    ) -> Result<ParentGrantStatus, ApplicationError> {
        let command = ParentAdoptionCommand::decode_scoped(bytes, self.parent_slots)?;
        let change = self.checked_parent(op, index, command)?;
        let GrantChange::Parent { status, .. } = change else {
            unreachable!()
        };
        if self.parent_adoption(op).is_none() {
            self.adoptions
                .try_reserve_exact(1)
                .map_err(|_| ApplicationError::ReceiptBudget)?;
            self.active_grant = change.after();
            self.adoptions.push(change);
        }
        Ok(status)
    }
}
