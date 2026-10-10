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
//! Parent changes leave original import and activation lineage immutable.
use super::*;
use crate::routed::{
    parent_adoption::ParentAdoptionCommand, MAX_CROSS_PARENT_ADOPTION_BYTES, MAX_PARENT_ADOPTIONS,
};
pub const PARENT_TRANSFER_TARGET_SCHEMA: u64 = 5;
const MAX_PARENT_TARGET_CHECKPOINT_BYTES: usize = 64 * 1024 * 1024;
#[derive(Clone)]
pub(super) struct ParentRecord {
    status: ParentGrantStatus,
    command: ParentAdoptionCommand,
}
pub(super) fn is_parent(bytes: &[u8]) -> bool {
    bytes.starts_with(b"VBRPAD01")
        || bytes.starts_with(b"VBXPAD01")
        || bytes.starts_with(b"VBMAAD01")
        || bytes.starts_with(b"VBMLAD01")
}
// Retired owners never serve again. Their compact lineage permits a round trip
// to the original parent while preserving every ownership/selector field.
pub(super) fn same_owner_lineage(
    grant: &ResponsibilityManifest,
    original: &ResponsibilityManifest,
) -> bool {
    if grant.input().parent.is_none() || original.input().parent.is_none() {
        return grant == original;
    }
    if grant.input().generation < original.input().generation {
        return false;
    }
    let mut normalized = grant.clone().into_input();
    normalized.parent = original.input().parent;
    normalized.generation = original.input().generation;
    normalized == *original.input()
}
pub(super) fn digest(op: OperationId, index: u64, command: &[u8]) -> ContentDigest {
    let mut b = Vec::with_capacity(32 + command.len());
    b.extend(b"VBTPARD1");
    b.extend(op.get().to_le_bytes());
    b.extend(index.to_le_bytes());
    b.extend(command);
    ContentDigest::sha256(&b)
}
impl<A, P> TransferTarget<A, P>
where
    A: ScopeStateMachine + BoundedStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    /// Select before bootstrap. Host-authenticated local and completed cross-
    /// authority observations use independent, bounded lifetime storage.
    #[allow(clippy::result_large_err)]
    pub fn with_parent_adoption(
        mut self,
        maximum: usize,
    ) -> Result<Self, (ApplicationError, Self)> {
        if maximum == 0
            || maximum > MAX_PARENT_ADOPTIONS
            || self.parent_limit != 0
            || self.partial.is_some()
            || self.applied != 0
            || self.staged.is_some()
            || self.binding.len() + 2 > MAX_INLINE_IMPORT_BYTES
            || self
                .readiness_requirements()
                .snapshot_bytes
                .checked_add(4 + maximum * (60 + MAX_CROSS_PARENT_ADOPTION_BYTES))
                .is_none_or(|n| n > MAX_PARENT_TARGET_CHECKPOINT_BYTES)
        {
            return Err((ApplicationError::InvalidCommand, self));
        }
        self.parent_limit = maximum;
        self.binding[..8].copy_from_slice(b"VBTSOWN4");
        self.binding.extend((maximum as u16).to_le_bytes());
        Ok(self)
    }
    pub fn metadata_adoption(
        &self,
        operation: OperationId,
    ) -> Option<crate::routed::MetadataGrantStatus> {
        self.parents.iter().find_map(|record| {
            if record.status.operation != operation {
                return None;
            }
            let ParentAdoptionCommand::Metadata(command) = &record.command else {
                return None;
            };
            Some(crate::routed::MetadataGrantStatus {
                owner: record.status,
                activation: command.activation(),
            })
        })
    }
    pub fn metadata_locator_adoption(
        &self,
        op: OperationId,
    ) -> Option<crate::routed::MetadataLocatorGrantStatus> {
        self.parents.iter().find_map(|r| match &r.command {
            ParentAdoptionCommand::Locator(c) if r.status.operation == op => {
                Some(c.status(r.status))
            }
            _ => None,
        })
    }
    pub fn grant(&self) -> &ResponsibilityManifest {
        &self.active_grant
    }
    pub fn parent_adoption(&self, op: OperationId) -> Option<ParentGrantStatus> {
        self.parents
            .iter()
            .find(|p| p.status.operation == op)
            .map(|p| p.status)
    }
    pub(super) fn parent_command_bound(&self) -> usize {
        if self.metadata_adoption {
            crate::routed::MAX_METADATA_ADOPTION_BYTES
        } else {
            MAX_CROSS_PARENT_ADOPTION_BYTES
        }
    }
    pub(super) fn parent_reserve(&self) -> usize {
        if self.parent_limit == 0 {
            0
        } else {
            2 + self.parent_limit * (60 + self.parent_command_bound())
        }
    }
    pub(super) fn apply_parent<R>(
        &mut self,
        op: OperationId,
        index: u64,
        bytes: &[u8],
    ) -> Result<TargetOutcome<R>, ApplicationError> {
        if self.parent_limit == 0 {
            return Err(ApplicationError::UnsupportedSchema);
        }
        let command = ParentAdoptionCommand::decode_metadata(
            bytes,
            true,
            self.metadata_adoption,
            self.metadata_locator_adoption,
        )?;
        self.apply_parent_command(op, index, command)
    }
    pub(super) fn apply_parent_command<R>(
        &mut self,
        op: OperationId,
        index: u64,
        command: ParentAdoptionCommand,
    ) -> Result<TargetOutcome<R>, ApplicationError> {
        if self.parent_limit == 0 {
            return Err(ApplicationError::UnsupportedSchema);
        }
        if let Some(old) = self.parents.iter().find(|p| p.status.operation == op) {
            return Ok(if old.command == command {
                TargetOutcome::ParentAdopted(old.status)
            } else {
                TargetOutcome::OperationConflict
            });
        }
        if op == self.operation
            || self.inner.contains_operation(op)
            || self
                .frozen
                .as_ref()
                .is_some_and(|f| f.fence.operation == op)
        {
            return Ok(TargetOutcome::OperationConflict);
        }
        if self.frozen.is_some() {
            return Ok(TargetOutcome::Rejected(RoutingError::Fenced));
        }
        if self.activated.is_none() {
            return Ok(TargetOutcome::NotActive);
        }
        if command.before() != self.grant() {
            return Ok(TargetOutcome::Rejected(RoutingError::WrongOwner));
        }
        if self.parents.len() >= self.parent_limit {
            return Err(ApplicationError::ReceiptBudget);
        }
        let status = ParentGrantStatus {
            operation: op,
            index,
            metadata_operation: command.metadata().0,
            metadata_index: command.metadata().1,
            generation: command.after().input().generation,
        };
        self.parents
            .try_reserve_exact(1)
            .map_err(|_| ApplicationError::ReceiptBudget)?;
        self.active_grant = command.after();
        self.parents.push(ParentRecord { status, command });
        Ok(TargetOutcome::ParentAdopted(status))
    }
    pub(super) fn parent_checkpoint(&self) -> Result<Vec<u8>, ApplicationError> {
        if self.parent_limit == 0 {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        out.extend((self.parents.len() as u16).to_le_bytes());
        for p in &self.parents {
            let command = p.command.encode(self.parent_command_bound())?;
            out.extend(digest(p.status.operation, p.status.index, &command).0);
            out.extend(p.status.operation.get().to_le_bytes());
            out.extend(p.status.index.to_le_bytes());
            out.extend((command.len() as u32).to_le_bytes());
            out.extend(command);
        }
        Ok(out)
    }
    pub(super) fn restore_parents(
        &mut self,
        r: &mut Reader<'_>,
        applied: u64,
        freeze: u64,
    ) -> Result<(), ApplicationError> {
        self.parents.clear();
        self.active_grant = self
            .intent
            .target_manifest(self.group)
            .expect("checked target")
            .clone();
        if self.parent_limit == 0 {
            return Ok(());
        }
        let count = r.u16()? as usize;
        if count > self.parent_limit {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut previous = self.activated.as_ref().map_or(0, |a| a.status.index);
        for _ in 0..count {
            let hash = r.take(32)?;
            let op = r.operation()?;
            let index = r.u64()?;
            let len = r.u32()? as usize;
            if len > self.parent_command_bound() {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            let command = r.take(len)?;
            if self.activated.is_none()
                || index <= previous
                || index > applied
                || index == u64::MAX
                || (freeze != 0 && index >= freeze)
                || hash != digest(op, index, command).0
                || self.parent_adoption(op).is_some()
            {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            // The checkpoint may be restored into an already-used instance.
            // Frozen state is reconstructed only after this ordered ledger.
            let old_frozen = self.frozen.take();
            let outcome = self.apply_parent::<()>(op, index, command);
            self.frozen = old_frozen;
            if !matches!(outcome?, TargetOutcome::ParentAdopted(_)) {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            previous = index;
        }
        Ok(())
    }
}
