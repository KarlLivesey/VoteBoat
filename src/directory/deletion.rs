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
    pub(super) fn deletion_busy(&self, id: ResponsibilityIdentity) -> bool {
        self.deletions.contains_key(&id)
    }
    pub(super) fn begin_deletion(
        &mut self,
        operation: OperationId,
        intent: DeletionIntent,
    ) -> DirectoryOutcome {
        let m = intent.before.input();
        let id = m.responsibility;
        if m.authority != self.plan.authority || !self.manifests.contains_key(&id) {
            return DirectoryOutcome::UnknownResponsibility;
        }
        if self.deletion_busy(id)
            || self.transfers.contains_key(&id)
            || self.delegations.contains_key(&id)
            || m.parent
                .filter(|p| p.group == self.plan.authority)
                .is_some_and(|p| {
                    self.transfers.contains_key(&p.responsibility)
                        || self.delegations.contains_key(&p.responsibility)
                })
        {
            return DirectoryOutcome::LifecycleBusy;
        }
        if self.manifests.get(&id) != Some(&intent.before) {
            return DirectoryOutcome::GenerationMismatch;
        }
        // Unconsumed creation may already have durable assigned stores. Do not
        // tombstone its delegator while that original reservation is unresolved.
        for (g, (_, op)) in &self.creations {
            if !self.namespace_publications.contains_key(op)
                && !self.insertion_creations.contains(op)
                && self
                    .group_creation_at(self.applied, *g)
                    .ok()
                    .flatten()
                    .is_some_and(|s| s.intent.parent == id)
            {
                return DirectoryOutcome::LifecycleBusy;
            }
        }
        if self.control_bytes + self.reserved_publication_bytes() + MAX_DELETION_COMPLETION_BYTES
            > self.control_history_capacity()
        {
            return DirectoryOutcome::TransferControlBusy;
        }
        self.deletions.insert(id, operation);
        DirectoryOutcome::DeletionIntentRecorded
    }
    pub fn deletion_intent_at(
        &self,
        required: u64,
        operation: OperationId,
    ) -> Result<Option<DeletionIntentStatus>, ApplicationError> {
        if !self.namespace_deletion {
            return Err(ApplicationError::UnsupportedSchema);
        }
        if required > self.applied {
            return Err(ApplicationError::NotApplied);
        }
        self.history
            .get(&operation)
            .filter(|h| h.outcome == DirectoryOutcome::DeletionIntentRecorded)
            .map(|h| {
                Ok(DeletionIntentStatus {
                    operation,
                    index: h.index,
                    intent: DeletionIntent::decode(&h.bytes)?,
                })
            })
            .transpose()
    }
    pub fn deletion_status_at(
        &self,
        required: u64,
        intent: OperationId,
    ) -> Result<Option<DeletionStatus>, ApplicationError> {
        if !self.namespace_deletion {
            return Err(ApplicationError::UnsupportedSchema);
        }
        if required > self.applied {
            return Err(ApplicationError::NotApplied);
        }
        self.deletion_publications
            .get(&intent)
            .map(|operation| {
                let h = &self.history[operation];
                Ok(DeletionStatus {
                    operation: *operation,
                    index: h.index,
                    intent: self
                        .deletion_intent_at(required, intent)?
                        .ok_or(ApplicationError::InvalidCommand)?,
                })
            })
            .transpose()
    }
    pub(super) fn deletion_permitted(
        &self,
        operation: OperationId,
        c: &DeletionCompletion,
    ) -> bool {
        let id = c.intent.intent.before.input().responsibility;
        if operation == c.intent.operation
            || c.validate().is_err()
            || self.deletion_publications.contains_key(&c.intent.operation)
            || self.deletions.get(&id) != Some(&c.intent.operation)
            || self.manifests.get(&id) != Some(&c.intent.intent.before)
            || self
                .deletion_intent_at(self.applied, c.intent.operation)
                .ok()
                .flatten()
                .as_ref()
                != Some(&c.intent)
        {
            return false;
        }
        c.children.iter().all(|f| {
            if f.authority != self.plan.authority {
                return true;
            }
            let Some(s) = self
                .deletion_status_at(self.applied, f.intent)
                .ok()
                .flatten()
            else {
                return false;
            };
            ChildDeletionEvidence::from_status(f.configuration, &s)
                .ok()
                .as_ref()
                == Some(f)
        })
    }
    pub(super) fn complete_deletion(
        &mut self,
        operation: OperationId,
        c: DeletionCompletion,
    ) -> DirectoryOutcome {
        if !self.deletion_permitted(operation, &c) {
            return DirectoryOutcome::TransferEvidenceMismatch;
        }
        let m = c.intent.intent.tombstone().expect("validated deletion");
        let generation = m.input().generation;
        self.manifests.insert(m.input().responsibility, m);
        self.deletion_publications
            .insert(c.intent.operation, operation);
        DirectoryOutcome::Deleted(generation)
    }
}
