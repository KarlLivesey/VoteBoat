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
impl Directory {
    fn idle_for_reparent(&self, id: ResponsibilityIdentity) -> bool {
        !self.deletion_busy(id)
            && !self.transfers.contains_key(&id)
            && !self.delegations.contains_key(&id)
            && !self.creations.iter().any(|(g, (_, op))| {
                !self.namespace_publications.contains_key(op)
                    && !self.insertion_creations.contains(op)
                    && self
                        .group_creation_at(self.applied, *g)
                        .ok()
                        .flatten()
                        .is_some_and(|s| s.intent.parent == id)
            })
    }
    pub(super) fn guarded_state_preserved(&self, before: &Self) -> bool {
        before.guarded_manifests.keys().all(|id| {
            self.manifests.get(id) == before.manifests.get(id) && self.idle_for_reparent(*id)
        })
    }
    pub(super) fn prepare_reparent(
        &mut self,
        operation: OperationId,
        p: PrepareReparent,
    ) -> DirectoryOutcome {
        if self.reparent_cancellations.contains_key(&operation) {
            return DirectoryOutcome::ReparentCancelled;
        }
        let authority = self.plan.authority;
        if !p.plan.authorities().contains(&authority) {
            return DirectoryOutcome::UnknownResponsibility;
        }
        if authority == p.plan.coordinator() {
            if p.coordinator.is_some() {
                return DirectoryOutcome::TransferEvidenceMismatch;
            }
        } else if p.coordinator.is_none_or(|c| {
            c.authority != p.plan.coordinator()
                || c.operation != operation
                || c.digest != p.plan.digest()
        }) {
            return DirectoryOutcome::TransferEvidenceMismatch;
        }
        let local = p
            .plan
            .manifests()
            .iter()
            .filter(|m| m.input().authority == authority)
            .collect::<Vec<_>>();
        for m in &local {
            let id = m.input().responsibility;
            if self.manifests.get(&id) != Some(*m) {
                return DirectoryOutcome::GenerationMismatch;
            }
            if self.guarded_manifests.contains_key(&id) || !self.idle_for_reparent(id) {
                return DirectoryOutcome::LifecycleBusy;
            }
        }
        if self.control_bytes
            + self.reserved_publication_bytes()
            + 2 * MAX_REPARENT_COMPLETION_BYTES
            > self.control_history_capacity()
        {
            return DirectoryOutcome::TransferControlBusy;
        }
        for m in local {
            self.guarded_manifests
                .insert(m.input().responsibility, operation);
        }
        self.guarded_operations.insert(operation);
        DirectoryOutcome::ReparentGuarded
    }
    pub fn reparent_guard_at(
        &self,
        required: u64,
        operation: OperationId,
    ) -> Result<Option<ReparentGuardStatus>, ApplicationError> {
        if !self.reparent_guards {
            return Err(ApplicationError::UnsupportedSchema);
        }
        if required > self.applied {
            return Err(ApplicationError::NotApplied);
        }
        self.history
            .get(&operation)
            .filter(|h| h.outcome == DirectoryOutcome::ReparentGuarded)
            .map(|h| {
                Ok(ReparentGuardStatus {
                    authority: self.plan.authority,
                    operation,
                    index: h.index,
                    plan: PrepareReparent::decode(&h.bytes)?.plan,
                })
            })
            .transpose()
    }
    pub fn reparent_guard_active(&self, operation: OperationId) -> bool {
        self.guarded_operations.contains(&operation)
    }
    pub(super) fn cancel_reparent_permitted(
        &self,
        operation: OperationId,
        c: CancelReparent,
    ) -> bool {
        operation != c.guard
            && self.guarded_operations.contains(&c.guard)
            && self
                .reparent_guard_at(self.applied, c.guard)
                .ok()
                .flatten()
                .is_some_and(|s| s.plan.coordinator() == self.plan.authority)
    }
    fn remove_reparent_guard(&mut self, guard: OperationId, record: OperationId) {
        self.guarded_manifests.retain(|_, op| *op != guard);
        self.guarded_operations.remove(&guard);
        self.reparent_cancellations.insert(guard, record);
    }
    pub(super) fn cancel_reparent(
        &mut self,
        _index: u64,
        operation: OperationId,
        c: CancelReparent,
    ) -> DirectoryOutcome {
        if !self.cancel_reparent_permitted(operation, c) {
            return DirectoryOutcome::TransferEvidenceMismatch;
        }
        self.remove_reparent_guard(c.guard, operation);
        DirectoryOutcome::ReparentCancelled
    }
    pub fn reparent_cancellation_at(
        &self,
        required: u64,
        guard: OperationId,
    ) -> Result<Option<ReparentCancellationStatus>, ApplicationError> {
        if !self.reparent_guards {
            return Err(ApplicationError::UnsupportedSchema);
        }
        if required > self.applied {
            return Err(ApplicationError::NotApplied);
        }
        let Some(record) = self.reparent_cancellations.get(&guard) else {
            return Ok(None);
        };
        let h = self
            .history
            .get(record)
            .ok_or(ApplicationError::InvalidCheckpoint)?;
        if h.bytes.starts_with(b"VBXRAB01") {
            return Ok(Some(ReleaseReparentGuard::decode(&h.bytes)?.decision));
        }
        let s = self
            .reparent_guard_at(required, guard)?
            .ok_or(ApplicationError::InvalidCheckpoint)?;
        Ok(Some(ReparentCancellationStatus {
            coordinator: self.plan.authority,
            guard,
            guard_index: s.index,
            digest: s.plan.digest(),
            operation: *record,
            index: h.index,
        }))
    }
    pub(super) fn release_reparent_permitted(
        &self,
        operation: OperationId,
        c: ReleaseReparentGuard,
    ) -> bool {
        let s = c.decision;
        if operation == s.guard || c.encode().is_err() {
            return false;
        }
        if let Ok(Some(old)) = self.reparent_cancellation_at(self.applied, s.guard) {
            return old == s;
        }
        if s.coordinator == self.plan.authority {
            return false;
        } // must be an actual local cancellation
        if let Ok(Some(local)) = self.reparent_guard_at(self.applied, s.guard) {
            local.plan.coordinator() == s.coordinator
                && local.plan.digest() == s.digest
                && self.guarded_operations.contains(&s.guard)
        } else {
            // The authenticated final coordinator decision may arrive before prepare.
            // Its tombstone prevents a late prepare from acquiring an orphan guard.
            true
        }
    }
    pub(super) fn release_reparent(
        &mut self,
        operation: OperationId,
        c: ReleaseReparentGuard,
    ) -> DirectoryOutcome {
        if !self.release_reparent_permitted(operation, c) {
            return DirectoryOutcome::TransferEvidenceMismatch;
        }
        if !self.reparent_cancellations.contains_key(&c.decision.guard) {
            self.remove_reparent_guard(c.decision.guard, operation);
        }
        DirectoryOutcome::ReparentCancelled
    }
    pub(super) fn admit_guarded<'a>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError> {
        let mut next = self.clone();
        for (i, (op, b)) in pending.enumerate() {
            if i >= MAX_DIRECTORY_PENDING {
                return Err(ApplicationError::DedupCapacity);
            }
            let index = next
                .applied
                .checked_add(1)
                .ok_or(ApplicationError::IndexGap)?;
            next.execute(index, op, b)?;
            next.applied = index;
        }
        let index = next
            .applied
            .checked_add(1)
            .ok_or(ApplicationError::IndexGap)?;
        let r = next.execute(index, operation, bytes)?;
        if !matches!(
            r.outcome,
            DirectoryOutcome::Initialized
                | DirectoryOutcome::CreationReserved
                | DirectoryOutcome::NamespacePublished(_)
                | DirectoryOutcome::Published(_)
                | DirectoryOutcome::TransferIntentRecorded
                | DirectoryOutcome::TransferPublished(_)
                | DirectoryOutcome::DelegationReserved
                | DirectoryOutcome::DelegationPublished(_)
                | DirectoryOutcome::DelegationDeclined
                | DirectoryOutcome::DelegationCancelled
                | DirectoryOutcome::DeletionIntentRecorded
                | DirectoryOutcome::Deleted(_)
                | DirectoryOutcome::ChildSlotRetired(_)
                | DirectoryOutcome::Reparented
                | DirectoryOutcome::ReparentGuarded
                | DirectoryOutcome::ReparentCancelled
        ) {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(size_of::<DirectoryReceipt>())
    }
}
