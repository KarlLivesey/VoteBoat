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
//! One-directory atomic refresh of foreign metadata references.
use super::*;
use crate::metadata_transfer::{MetadataLocatorStatus, MetadataLocatorUpdate};
use crate::transfer::ContentDigest;

impl Directory {
    pub(super) fn locator_outcome(&self, update: &MetadataLocatorUpdate) -> DirectoryOutcome {
        let before = update.before();
        let id = before.input().responsibility;
        if before.input().authority != self.plan.authority || !self.manifests.contains_key(&id) {
            return DirectoryOutcome::UnknownResponsibility;
        }
        if self.manifests.get(&id) != Some(before) {
            return DirectoryOutcome::GenerationMismatch;
        }
        // A changed local ancestor also invalidates plans that captured it.
        // Freeze this small administrative step while any lifecycle is open.
        if self.reserved_publication_bytes() != 0 || !self.guarded_operations.is_empty() {
            return DirectoryOutcome::LifecycleBusy;
        }
        DirectoryOutcome::MetadataLocatorUpdated(update.after().input().generation)
    }
    pub(super) fn locator_permitted(&self, update: &MetadataLocatorUpdate, bytes: usize) -> bool {
        matches!(
            self.locator_outcome(update),
            DirectoryOutcome::MetadataLocatorUpdated(_)
        ) && self.metadata_locator_controls.len() < self.limits.operations
            && self.control_bytes + bytes + self.reserved_publication_bytes()
                <= self.control_history_capacity()
    }
    pub(super) fn update_metadata_locators(
        &mut self,
        update: MetadataLocatorUpdate,
    ) -> DirectoryOutcome {
        let outcome = self.locator_outcome(&update);
        if matches!(outcome, DirectoryOutcome::MetadataLocatorUpdated(_)) {
            let after = update.after();
            self.manifests.insert(after.input().responsibility, after);
        }
        outcome
    }
    /// Query through the original authority's quorum for use in another group.
    pub fn metadata_locator_at(
        &self,
        required: u64,
        operation: OperationId,
    ) -> Result<Option<MetadataLocatorStatus>, ApplicationError> {
        if !self.metadata_locators {
            return Err(ApplicationError::UnsupportedSchema);
        }
        if required > self.applied {
            return Err(ApplicationError::NotApplied);
        }
        self.history
            .get(&operation)
            .filter(|h| matches!(h.outcome, DirectoryOutcome::MetadataLocatorUpdated(_)))
            .map(|h| {
                let command = MetadataLocatorUpdate::decode(&h.bytes)?;
                Ok(MetadataLocatorStatus {
                    operation,
                    index: h.index,
                    authority: self.plan.authority,
                    responsibility: command.before().input().responsibility,
                    generation: command.after().input().generation,
                    command_digest: ContentDigest::sha256(&h.bytes),
                    activation: command.activation(),
                })
            })
            .transpose()
    }
}
