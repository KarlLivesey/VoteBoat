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
impl ApplicationReceipt for LogicalReceipt {
    fn index(&self) -> u64 {
        self.index
    }
    fn operation(&self) -> OperationId {
        self.operation
    }
    fn nested_bytes(&self, _: usize) -> Result<usize, ApplicationError> {
        Ok(0)
    }
}
impl<C: LogCodec + Clone, S: SnapshotCodec + Clone, W: WireCodec + Clone> StateMachine
    for LogicalApplication<C, S, W>
{
    type Receipt = LogicalReceipt;
    fn validate_group(&self, group: GroupIdentity) -> Result<(), ApplicationError> {
        if group == self.mapping.child {
            Ok(())
        } else {
            Err(ApplicationError::InvalidCommand)
        }
    }
    /// Configuration entries kept for life until AI3 (audit F5); actions retire by the LV2 watermark.
    fn retention(&self) -> crate::application::Retention {
        crate::application::Retention::LifetimeBounded
    }
    fn deployment_requirements(&self) -> Option<crate::raft::ReadinessRequirements> {
        Some(crate::raft::ReadinessRequirements {
            application_schema: self.application_schema(),
            inner_profile: crate::raft::InnerProfile::LEAF,
            command_bytes: self.limits.max_command_bytes,
            snapshot_bytes: self.limits.max_child_checkpoint_bytes,
        })
    }
    fn applied_index(&self) -> u64 {
        self.state.applied
    }
    fn apply_batch(
        &mut self,
        entries: &[LogEntry],
    ) -> Result<Vec<LogicalReceipt>, ApplicationError> {
        // The test journal stays out of the working copy.
        #[cfg(test)]
        let mut journal = self.journal.take();
        let mut next = self.clone();
        let result = next.apply_entries(entries);
        #[cfg(test)]
        {
            if let (Ok(_), Some(journal)) = (&result, &mut journal) {
                journal.extend_from_slice(entries);
            }
            self.journal = journal;
        }
        let receipts = result?;
        self.state = next.state;
        Ok(receipts)
    }
}
impl<C: LogCodec + Clone, S: SnapshotCodec + Clone, W: WireCodec + Clone> BoundedStateMachine
    for LogicalApplication<C, S, W>
{
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        entries
            .iter()
            .filter(|e| matches!(e.payload, EntryPayload::Command { .. }))
            .count()
            .checked_mul(std::mem::size_of::<LogicalReceipt>())
            .ok_or(ApplicationError::ReceiptBudget)
    }
}
