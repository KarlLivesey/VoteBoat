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
    pub(super) fn retire_child_slot(&mut self, r: RetireChildSlot) -> DirectoryOutcome {
        let b = r.before.input();
        let id = b.responsibility;
        if b.authority != self.plan.authority || !self.manifests.contains_key(&id) {
            return DirectoryOutcome::UnknownResponsibility;
        }
        if self.deletion_busy(id)
            || self.transfers.contains_key(&id)
            || self.delegations.contains_key(&id)
        {
            return DirectoryOutcome::LifecycleBusy;
        }
        // A pending creation still consumes the original parent generation.
        // Preserve its publication path instead of orphaning the reservation.
        if self.creations.iter().any(|(g, (_, op))| {
            !self.namespace_publications.contains_key(op)
                && !self.insertion_creations.contains(op)
                && self
                    .group_creation_at(self.applied, *g)
                    .ok()
                    .flatten()
                    .is_some_and(|s| s.intent.parent == id)
        }) {
            return DirectoryOutcome::LifecycleBusy;
        }
        if self.manifests.get(&id) != Some(&r.before) {
            return DirectoryOutcome::GenerationMismatch;
        }
        if r.child.authority == self.plan.authority {
            let Some(status) = self
                .deletion_status_at(self.applied, r.child.intent)
                .ok()
                .flatten()
            else {
                return DirectoryOutcome::TransferEvidenceMismatch;
            };
            if ChildDeletionEvidence::from_status(r.child.configuration, &status)
                .ok()
                .as_ref()
                != Some(&r.child)
            {
                return DirectoryOutcome::TransferEvidenceMismatch;
            }
        }
        let Ok(after) = r.after() else {
            return DirectoryOutcome::TransferEvidenceMismatch;
        };
        let gen = after.input().generation;
        self.retired_slot_children
            .insert(r.child.responsibility, r.child.authority);
        self.manifests.insert(id, after);
        DirectoryOutcome::ChildSlotRetired(gen)
    }
    pub fn retired_child_slot_at(
        &self,
        required: u64,
        operation: OperationId,
    ) -> Result<Option<RetiredChildSlotStatus>, ApplicationError> {
        if !self.child_slot_retirement {
            return Err(ApplicationError::UnsupportedSchema);
        }
        if required > self.applied {
            return Err(ApplicationError::NotApplied);
        }
        self.history
            .get(&operation)
            .filter(|h| matches!(h.outcome, DirectoryOutcome::ChildSlotRetired(_)))
            .map(|h| {
                Ok(RetiredChildSlotStatus {
                    operation,
                    index: h.index,
                    retirement: RetireChildSlot::decode(&h.bytes)?,
                })
            })
            .transpose()
    }
    pub(super) fn retired_child_identity_used(
        &self,
        group: GroupIdentity,
        child: ResponsibilityIdentity,
    ) -> bool {
        self.retired_slot_children
            .iter()
            .any(|(c, g)| c.id == child.id || g.id == group.id)
    }
}
