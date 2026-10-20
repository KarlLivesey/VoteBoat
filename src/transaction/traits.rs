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
use crate::{
    application::*,
    identity::OperationId,
    log::{EntryPayload, LogEntry},
    raft::ReadinessRequirements,
};
use std::mem::size_of;

impl ApplicationReceipt for TransferReceipt {
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
impl StateMachine for TransferApplication {
    type Receipt = TransferReceipt;
    fn validate_group(&self, group: GroupIdentity) -> Result<(), ApplicationError> {
        if group == self.group {
            Ok(())
        } else {
            Err(ApplicationError::InvalidCommand)
        }
    }
    /// Lifetime transaction records until TX1 (audit S5).
    fn retention(&self) -> crate::application::Retention {
        crate::application::Retention::LifetimeBounded
    }
    fn deployment_requirements(&self) -> Option<ReadinessRequirements> {
        use crate::raft::DeploymentPlan;
        // Plan: this group, which with the profile decides the role, then the
        // profile's coordinator and two participants (3 words each); then a
        // participant's opening image, per account in id order: id, opening
        // (an initial input no committed entry binds). 1 + 12 + 2a words, so
        // at most 89 accounts fit; a larger image overflows the profile and
        // readiness refuses it rather than truncating.
        let [first, second] = self.profile.participants();
        let mut plan = Vec::with_capacity(12 + 2 * self.accounts.len());
        for group in [self.group, self.profile.coordinator(), first, second] {
            plan.extend(DeploymentPlan::group_words(group));
        }
        for (id, account) in &self.accounts {
            plan.extend([id.get(), account.opening as u64]);
        }
        Some(ReadinessRequirements {
            application_schema: TRANSFER_SCHEMA,
            // Words: transaction and account capacities, each of which
            // decides whether a committed command is recorded.
            identity: crate::raft::LayerIdentity::new(
                crate::raft::ApplicationFamily::TRANSFER_TRANSACTION,
                [
                    self.limits.transactions() as u64,
                    self.limits.accounts() as u64,
                    0,
                    0,
                ],
            ),
            inner_profile: crate::raft::InnerProfile::LEAF.planned(&plan),
            command_bytes: TRANSFER_COMMAND_BYTES,
            snapshot_bytes: codec::checkpoint_bound(self),
        })
    }
    fn applied_index(&self) -> u64 {
        self.applied
    }
    fn apply_batch(
        &mut self,
        entries: &[LogEntry],
    ) -> Result<Vec<TransferReceipt>, ApplicationError> {
        let count = entries
            .iter()
            .filter(|e| matches!(e.payload, EntryPayload::Command { .. }))
            .count();
        let mut receipts = Vec::with_capacity(count);
        let mut next = self.clone();
        for entry in entries {
            if next.applied.checked_add(1) != Some(entry.index) {
                return Err(ApplicationError::IndexGap);
            }
            if entry.term == 0 {
                return Err(ApplicationError::InvalidCommand);
            }
            if let EntryPayload::Command { operation, bytes } = &entry.payload {
                let command = codec::decode(bytes)?;
                next.check_command(*operation, command)?;
                let (outcome, duplicate, first_index) = next.execute(command, entry.index)?;
                receipts.push(TransferReceipt {
                    index: entry.index,
                    first_index,
                    operation: *operation,
                    outcome,
                    duplicate,
                });
            }
            next.applied = entry.index;
        }
        *self = next;
        Ok(receipts)
    }
}
impl BoundedStateMachine for TransferApplication {
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        entries
            .iter()
            .filter(|e| matches!(e.payload, EntryPayload::Command { .. }))
            .count()
            .checked_mul(size_of::<TransferReceipt>())
            .ok_or(ApplicationError::ReceiptBudget)
    }
}
impl ReadableStateMachine for TransferApplication {
    type Query = TransferQuery;
    type ReadResult = TransferRead;
    fn read_at(
        &self,
        required_index: u64,
        query: TransferQuery,
    ) -> Result<TransferRead, ApplicationError> {
        if required_index > self.applied {
            return Err(ApplicationError::NotApplied);
        }
        Ok(match query {
            TransferQuery::Status(id) => TransferRead::Status(self.status(id).map(Box::new)),
            TransferQuery::Account(id) => TransferRead::Account(self.account(id)),
        })
    }
}
impl BoundedReadableStateMachine for TransferApplication {
    fn query_bytes(&self, _: &TransferQuery, _: usize) -> Result<usize, ApplicationError> {
        Ok(0)
    }
    fn read_result_bound(&self, query: &TransferQuery) -> Result<usize, ApplicationError> {
        Ok(size_of::<TransferRead>()
            + if matches!(query, TransferQuery::Status(_)) {
                size_of::<TransferStatus>()
            } else {
                0
            })
    }
    fn read_result_bytes(
        &self,
        result: &TransferRead,
        nested_limit: usize,
    ) -> Result<usize, ApplicationError> {
        let bytes = if matches!(result, TransferRead::Status(Some(_))) {
            size_of::<TransferStatus>()
        } else {
            0
        };
        if bytes > nested_limit {
            return Err(ApplicationError::ReceiptBudget);
        }
        Ok(bytes)
    }
}
impl CheckpointStateMachine for TransferApplication {
    fn schema_version(&self) -> u64 {
        TRANSFER_SCHEMA
    }
    fn checkpoint(&self, max_bytes: usize) -> Result<Vec<u8>, ApplicationError> {
        codec::checkpoint(self, max_bytes)
    }
    fn restore_checkpoint(
        &mut self,
        schema: u64,
        applied_index: u64,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        codec::restore(self, schema, applied_index, bytes)
    }
}
