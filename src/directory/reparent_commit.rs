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
    pub(super) fn reparent_reserved_bytes(&self) -> usize {
        self.guarded_operations
            .iter()
            .map(|op| {
                if self.reparent_publications.contains_key(op) {
                    1
                } else {
                    2
                }
            })
            .sum::<usize>()
            * MAX_REPARENT_COMPLETION_BYTES
    }
    pub(super) fn checked_reparent_update(
        &self,
        before: &Self,
        id: ResponsibilityIdentity,
    ) -> bool {
        let Some(guard) = before.guarded_manifests.get(&id) else {
            return false;
        };
        if !self.reparent_publications.contains_key(guard)
            || before.reparent_publications.contains_key(guard)
        {
            return false;
        }
        before
            .reparent_guard_at(before.applied, *guard)
            .ok()
            .flatten()
            .is_some_and(|s| {
                s.plan
                    .updated_manifests()
                    .into_iter()
                    .any(|m| m.input().responsibility == id && self.manifests.get(&id) == Some(&m))
            })
    }
    fn checked_guard_set(&self, c: &CommitReparent) -> Option<ReparentGuardStatus> {
        c.encode(MAX_REPARENT_COMPLETION_BYTES).ok()?;
        let local = self.reparent_guard_at(self.applied, c.guard).ok()??;
        if !self.guarded_operations.contains(&c.guard)
            || self.reparent_cancellations.contains_key(&c.guard)
            || c.guards.iter().map(|e| e.authority).collect::<Vec<_>>() != local.plan.authorities()
            || c.guards.iter().any(|e| e.digest != local.plan.digest())
        {
            return None;
        }
        let own = c
            .guards
            .iter()
            .find(|e| e.authority == self.plan.authority)?;
        if own.index != local.index {
            return None;
        }
        let prepare = PrepareReparent::decode(&self.history.get(&c.guard)?.bytes).ok()?;
        if let Some(first) = prepare.coordinator {
            if c.guards[0] != first {
                return None;
            }
        }
        if local
            .plan
            .manifests()
            .iter()
            .filter(|m| m.input().authority == self.plan.authority)
            .any(|m| {
                self.guarded_manifests.get(&m.input().responsibility) != Some(&c.guard)
                    || self.manifests.get(&m.input().responsibility) != Some(m)
            })
        {
            return None;
        }
        Some(local)
    }
    pub(super) fn commit_reparent_permitted(
        &self,
        operation: OperationId,
        c: &CommitReparent,
    ) -> bool {
        operation != c.guard
            && !self.reparent_decisions.contains_key(&c.guard)
            && !self.reparent_publications.contains_key(&c.guard)
            && self
                .checked_guard_set(c)
                .is_some_and(|s| s.plan.coordinator() == self.plan.authority)
    }
    fn apply_reparent_local(&mut self, plan: &CrossReparentPlan) {
        for m in plan.updated_manifests() {
            if m.input().authority == self.plan.authority {
                self.manifests.insert(m.input().responsibility, m);
            }
        }
    }
    pub(super) fn commit_reparent(
        &mut self,
        _index: u64,
        operation: OperationId,
        c: CommitReparent,
    ) -> DirectoryOutcome {
        if !self.commit_reparent_permitted(operation, &c) {
            return DirectoryOutcome::TransferEvidenceMismatch;
        }
        let s = self
            .checked_guard_set(&c)
            .expect("checked original guard set");
        self.apply_reparent_local(&s.plan);
        self.reparent_decisions.insert(c.guard, operation);
        self.reparent_publications.insert(c.guard, operation);
        DirectoryOutcome::ReparentCommitted
    }
    pub fn reparent_decision_at(
        &self,
        required: u64,
        guard: OperationId,
    ) -> Result<Option<ReparentDecisionStatus>, ApplicationError> {
        if !self.cross_reparenting {
            return Err(ApplicationError::UnsupportedSchema);
        }
        if required > self.applied {
            return Err(ApplicationError::NotApplied);
        }
        if let Some(record) = self.reparent_decisions.get(&guard) {
            let h = self
                .history
                .get(record)
                .ok_or(ApplicationError::InvalidCheckpoint)?;
            return Ok(Some(ReparentDecisionStatus {
                coordinator: self.plan.authority,
                operation: *record,
                index: h.index,
                commit: CommitReparent::decode(&h.bytes)?,
            }));
        }
        self.reparent_publications
            .get(&guard)
            .map(|record| {
                let h = self
                    .history
                    .get(record)
                    .ok_or(ApplicationError::InvalidCheckpoint)?;
                Ok(PublishReparent::decode(&h.bytes)?.decision)
            })
            .transpose()
    }
    pub(super) fn publish_reparent_permitted(
        &self,
        operation: OperationId,
        c: &PublishReparent,
    ) -> bool {
        if operation == c.decision.commit.guard
            || c.encode(MAX_REPARENT_COMPLETION_BYTES).is_err()
            || self
                .reparent_publications
                .contains_key(&c.decision.commit.guard)
        {
            return false;
        }
        let Some(s) = self.checked_guard_set(&c.decision.commit) else {
            return false;
        };
        if c.decision.coordinator != s.plan.coordinator() {
            return false;
        }
        if c.decision.coordinator == self.plan.authority {
            return self
                .reparent_decision_at(self.applied, c.decision.commit.guard)
                .ok()
                .flatten()
                .as_ref()
                == Some(&c.decision);
        }
        true
    }
    pub(super) fn publish_reparent(
        &mut self,
        operation: OperationId,
        c: PublishReparent,
    ) -> DirectoryOutcome {
        if !self.publish_reparent_permitted(operation, &c) {
            return DirectoryOutcome::TransferEvidenceMismatch;
        }
        let local = self
            .checked_guard_set(&c.decision.commit)
            .expect("checked original local guard");
        self.apply_reparent_local(&local.plan);
        self.reparent_publications
            .insert(c.decision.commit.guard, operation);
        DirectoryOutcome::ReparentPublished
    }
    pub fn reparent_publication_at(
        &self,
        required: u64,
        guard: OperationId,
    ) -> Result<Option<ReparentPublicationStatus>, ApplicationError> {
        if !self.cross_reparenting {
            return Err(ApplicationError::UnsupportedSchema);
        }
        if required > self.applied {
            return Err(ApplicationError::NotApplied);
        }
        let Some(record) = self.reparent_publications.get(&guard) else {
            return Ok(None);
        };
        let h = self
            .history
            .get(record)
            .ok_or(ApplicationError::InvalidCheckpoint)?;
        let decision = self
            .reparent_decision_at(required, guard)?
            .ok_or(ApplicationError::InvalidCheckpoint)?;
        Ok(Some(ReparentPublicationStatus {
            authority: self.plan.authority,
            operation: *record,
            index: h.index,
            guard,
            decision_digest: decision.digest()?,
        }))
    }
    pub(super) fn finish_reparent_permitted(
        &self,
        operation: OperationId,
        c: &FinishReparent,
    ) -> bool {
        if operation == c.guard
            || c.encode(MAX_REPARENT_COMPLETION_BYTES).is_err()
            || !self.guarded_operations.contains(&c.guard)
            || self.reparent_completions.contains_key(&c.guard)
        {
            return false;
        }
        let Ok(Some(d)) = self.reparent_decision_at(self.applied, c.guard) else {
            return false;
        };
        if d.coordinator != self.plan.authority
            || operation == d.operation
            || c.publications
                .iter()
                .map(|e| e.authority)
                .collect::<Vec<_>>()
                != d.commit
                    .guards
                    .iter()
                    .map(|e| e.authority)
                    .collect::<Vec<_>>()
        {
            return false;
        }
        let Ok(digest) = d.digest() else { return false };
        if c.publications
            .iter()
            .zip(&d.commit.guards)
            .any(|(e, g)| e.decision_digest != digest || e.index <= g.index)
        {
            return false;
        }
        let Ok(Some(local)) = self.reparent_publication_at(self.applied, c.guard) else {
            return false;
        };
        c.publications
            .iter()
            .find(|e| e.authority == self.plan.authority)
            .is_some_and(|e| e.index == local.index)
    }
    fn release_committed_guard(&mut self, guard: OperationId, operation: OperationId) {
        self.guarded_manifests.retain(|_, op| *op != guard);
        self.guarded_operations.remove(&guard);
        self.reparent_completions.insert(guard, operation);
    }
    pub(super) fn finish_reparent(
        &mut self,
        operation: OperationId,
        c: FinishReparent,
    ) -> DirectoryOutcome {
        if !self.finish_reparent_permitted(operation, &c) {
            return DirectoryOutcome::TransferEvidenceMismatch;
        }
        self.release_committed_guard(c.guard, operation);
        DirectoryOutcome::ReparentCompleted
    }
    pub fn reparent_completion_at(
        &self,
        required: u64,
        guard: OperationId,
    ) -> Result<Option<ReparentCompletionStatus>, ApplicationError> {
        if !self.cross_reparenting {
            return Err(ApplicationError::UnsupportedSchema);
        }
        if required > self.applied {
            return Err(ApplicationError::NotApplied);
        }
        let Some(record) = self.reparent_completions.get(&guard) else {
            return Ok(None);
        };
        let h = self
            .history
            .get(record)
            .ok_or(ApplicationError::InvalidCheckpoint)?;
        if h.bytes.starts_with(b"VBXRRL01") {
            return Ok(Some(ReleaseCommittedReparent::decode(&h.bytes)?.completion));
        }
        let d = self
            .reparent_decision_at(required, guard)?
            .ok_or(ApplicationError::InvalidCheckpoint)?;
        Ok(Some(ReparentCompletionStatus {
            coordinator: self.plan.authority,
            operation: *record,
            index: h.index,
            guard,
            decision_digest: d.digest()?,
        }))
    }
    pub(super) fn release_committed_permitted(
        &self,
        operation: OperationId,
        c: ReleaseCommittedReparent,
    ) -> bool {
        let s = c.completion;
        if operation == s.guard
            || c.encode().is_err()
            || !self.guarded_operations.contains(&s.guard)
            || self.reparent_completions.contains_key(&s.guard)
            || self.reparent_cancellations.contains_key(&s.guard)
        {
            return false;
        }
        let Ok(Some(d)) = self.reparent_decision_at(self.applied, s.guard) else {
            return false;
        };
        if d.coordinator != s.coordinator
            || d.digest().ok() != Some(s.decision_digest)
            || s.index <= d.index
            || s.operation == d.operation
        {
            return false;
        }
        if s.coordinator == self.plan.authority {
            return self
                .reparent_completion_at(self.applied, s.guard)
                .ok()
                .flatten()
                == Some(s);
        }
        true
    }
    pub(super) fn release_committed_reparent(
        &mut self,
        operation: OperationId,
        c: ReleaseCommittedReparent,
    ) -> DirectoryOutcome {
        if !self.release_committed_permitted(operation, c) {
            return DirectoryOutcome::TransferEvidenceMismatch;
        }
        self.release_committed_guard(c.completion.guard, operation);
        DirectoryOutcome::ReparentReleased
    }
}
