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
//! Private deterministic routed control application and pending admission replay.
use super::*;
impl<A, P> RoutedApplication<A, P>
where
    A: CheckpointStateMachine + BoundedStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    pub(super) fn apply_bootstrap_control(
        &mut self,
        operation: OperationId,
        index: u64,
    ) -> RoutedOutcome<A::Receipt> {
        if self.initialized.is_some_and(|(op, _)| op != operation)
            || self.history.contains_key(&operation)
            || self.scoped_operation(operation)
            || self.parent_operation(operation)
            || self.fence.is_some_and(|f| f.operation == operation)
        {
            RoutedOutcome::OperationConflict
        } else {
            self.initialized.get_or_insert((operation, index));
            RoutedOutcome::Bootstrapped
        }
    }
    pub(super) fn apply_full_fence(
        &mut self,
        operation: OperationId,
        index: u64,
        epoch: OwnershipEpoch,
    ) -> RoutedOutcome<A::Receipt> {
        if epoch != self.grant.input().epoch {
            RoutedOutcome::Rejected(RoutingError::EpochMismatch)
        } else if self.initialized.is_some_and(|(op, _)| op == operation)
            || self.history.contains_key(&operation)
            || self.scoped_operation(operation)
            || self.parent_operation(operation)
        {
            RoutedOutcome::OperationConflict
        } else if let Some(fence) = self.fence {
            if fence.operation == operation {
                RoutedOutcome::Fenced(fence)
            } else {
                RoutedOutcome::Rejected(RoutingError::Fenced)
            }
        } else {
            let fence = OwnershipFence {
                group: self.local,
                responsibility: self.grant.input().responsibility,
                epoch,
                operation,
                index,
            };
            self.fence = Some(fence);
            RoutedOutcome::Fenced(fence)
        }
    }
    pub(super) fn apply_scope_fence(
        &mut self,
        operation: OperationId,
        index: u64,
        epoch: OwnershipEpoch,
        scope: BucketRange,
    ) -> Result<RoutedOutcome<A::Receipt>, ApplicationError> {
        let outcome = if epoch != self.grant.input().epoch {
            RoutedOutcome::Rejected(RoutingError::EpochMismatch)
        } else if self.initialized.is_some_and(|(op, _)| op == operation)
            || self.history.contains_key(&operation)
        {
            RoutedOutcome::OperationConflict
        } else if let Some(f) = self
            .scope_fences
            .iter()
            .find(|s| s.fence.operation == operation)
        {
            if f.scope == scope {
                RoutedOutcome::ScopeFenced(*f)
            } else {
                RoutedOutcome::OperationConflict
            }
        } else if self.fence.is_some()
            || self
                .scope_fences
                .iter()
                .any(|s| s.scope.start() < scope.end() && scope.start() < s.scope.end())
        {
            RoutedOutcome::Rejected(RoutingError::Fenced)
        } else if !self.owns_scope(scope) {
            RoutedOutcome::Rejected(RoutingError::WrongOwner)
        } else if self.scope_fences.len() == self.scope_limit {
            return Err(ApplicationError::ReceiptBudget);
        } else {
            let f = ScopedOwnershipFence {
                scope,
                fence: OwnershipFence {
                    group: self.local,
                    responsibility: self.grant.input().responsibility,
                    epoch,
                    operation,
                    index,
                },
            };
            self.scope_fences
                .try_reserve_exact(1)
                .map_err(|_| ApplicationError::ReceiptBudget)?;
            if self.scope_fences.capacity() > MAX_MANIFEST_ROUTES {
                return Err(ApplicationError::ReceiptBudget);
            }
            self.scope_fences.push(f);
            RoutedOutcome::ScopeFenced(f)
        };
        Ok(outcome)
    }
    pub(super) fn validate_replayed_proposal<'a>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError>
    where
        A: ProposalAdmission,
    {
        if bytes.len() > MAX_ROUTED_COMMAND_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut next = self.clone();
        for (position, (id, pending_bytes)) in pending.enumerate() {
            if position >= MAX_ROUTED_PENDING {
                return Err(ApplicationError::ReceiptBudget);
            }
            if pending_bytes.len() > MAX_ROUTED_COMMAND_BYTES {
                return Err(ApplicationError::InvalidCommand);
            }
            let entry = LogEntry {
                index: next
                    .applied_index()
                    .checked_add(1)
                    .ok_or(ApplicationError::IndexGap)?,
                term: 1,
                payload: EntryPayload::Command {
                    operation: id,
                    bytes: pending_bytes.to_vec(),
                },
            };
            next.apply_batch(&[entry])?;
        }
        let entry = LogEntry {
            index: next
                .applied_index()
                .checked_add(1)
                .ok_or(ApplicationError::IndexGap)?,
            term: 1,
            payload: EntryPayload::Command {
                operation,
                bytes: bytes.to_vec(),
            },
        };
        if let Command::Data { hint, key, payload } = next.request(bytes)? {
            next.check_context(&hint, key)
                .map_err(|_| ApplicationError::InvalidCommand)?;
            if next.semantic_conflict(operation, key, payload) {
                return Err(ApplicationError::InvalidCommand);
            }
            next.inner
                .validate_proposal(operation, payload, std::iter::empty())?;
        }
        let bound = next.receipt_bytes_bound(std::slice::from_ref(&entry))?;
        let receipts = next.apply_batch(&[entry])?;
        if matches!(
            receipts[0].outcome,
            RoutedOutcome::Rejected(_) | RoutedOutcome::OperationConflict
        ) {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(bound)
    }
}
