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
//! Metadata-authority adoption and replayable retirement lineage.
use super::*;
use crate::routed::{
    parent_adoption::ParentAdoptionCommand, MAX_METADATA_ADOPTION_BYTES, MAX_PARENT_ADOPTIONS,
};
pub const METADATA_TRANSFER_TARGET_SCHEMA: u64 = 8;
pub const METADATA_PARTIAL_TRANSFER_TARGET_SCHEMA: u64 = 9;
impl<A, P> TransferTarget<A, P>
where
    A: ScopeStateMachine + BoundedStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    /// Select before bootstrap and before partial delegation. Complete metadata
    /// observations share the ordered parent ledger and its lifetime slot limit.
    #[allow(clippy::result_large_err)]
    pub fn with_metadata_authority_adoption(
        self,
        maximum: usize,
    ) -> Result<Self, (ApplicationError, Self)> {
        if maximum == 0
            || maximum > MAX_PARENT_ADOPTIONS
            || self
                .readiness_requirements()
                .snapshot_bytes
                .checked_add(4 + maximum * (60 + MAX_METADATA_ADOPTION_BYTES))
                .is_none_or(|n| n > MAX_SCOPE_IMAGE_BYTES)
        {
            return Err((ApplicationError::InvalidCommand, self));
        }
        let mut next = self.with_parent_adoption(maximum)?;
        next.metadata_adoption = true;
        next.binding[..8].copy_from_slice(b"VBTSOWN6");
        Ok(next)
    }
    pub(super) fn metadata_retirement_bound(&self) -> usize {
        22 + MAX_TARGET_ACTIVATION_BYTES + self.parent_limit * (60 + self.parent_command_bound())
    }
    pub(super) fn metadata_retirement_lineage(&self) -> Result<Vec<u8>, ApplicationError> {
        let activation = self
            .activated
            .as_ref()
            .ok_or(ApplicationError::NotApplied)?;
        let parents = self.parent_checkpoint()?;
        let len = 20 + activation.bytes.len() + parents.len();
        if len > self.metadata_retirement_bound() {
            return Err(ApplicationError::ReceiptBudget);
        }
        let mut bytes = Vec::with_capacity(len);
        bytes.extend(activation.status.index.to_le_bytes());
        bytes.extend((activation.bytes.len() as u32).to_le_bytes());
        bytes.extend(&activation.bytes);
        bytes.extend(b"VBTPMRL1");
        bytes.extend(parents);
        Ok(bytes)
    }
    pub(super) fn metadata_retirement_grant(
        &self,
        bytes: &[u8],
        fence: u64,
    ) -> Result<ResponsibilityManifest, ApplicationError> {
        if bytes.len() > self.metadata_retirement_bound() {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut r = Reader::new(bytes);
        let mut previous = r.u64()?;
        let n = r.u32()? as usize;
        let activation = self.activation(r.take(n)?)?;
        let publication = &activation.decision.publication;
        let imported = publication
            .targets()
            .iter()
            .find(|t| t.group == self.group)
            .ok_or(ApplicationError::InvalidCheckpoint)?;
        if publication.operation() != self.operation
            || publication.intent() != &self.intent
            || previous <= imported.imported.index
            || previous >= fence
            || fence == u64::MAX
            || r.take(8)? != b"VBTPMRL1"
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let count = r.u16()? as usize;
        if count > self.parent_limit {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut used = std::collections::BTreeSet::from([self.operation]);
        let mut grant = self.intent.target_manifest(self.group).unwrap().clone();
        for _ in 0..count {
            let hash = r.take(32)?;
            let operation = r.operation()?;
            let index = r.u64()?;
            let n = r.u32()? as usize;
            if n > self.parent_command_bound()
                || index <= previous
                || index >= fence
                || !used.insert(operation)
            {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            let bytes = r.take(n)?;
            if hash != parent::digest(operation, index, bytes).0 {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            let command =
                ParentAdoptionCommand::decode_metadata(bytes, true, self.metadata_adoption)?;
            if command.before() != &grant || command.encode(n)? != bytes {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            grant = command.after();
            previous = index;
        }
        if !r.done() {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        Ok(grant)
    }
}
