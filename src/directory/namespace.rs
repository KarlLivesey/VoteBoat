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
    pub(super) fn namespace_permitted(&self, p: &NamespacePublication) -> bool {
        if !self.namespace_creation
            || self.namespace_publications.contains_key(&p.creation)
            || self.plan.manifests.len()
                + self.namespace_publications.len()
                + self.insertion_creations.len()
                >= MAX_DIRECTORY_MANIFESTS
            || self
                .manifests
                .keys()
                .any(|id| id.id == p.manifest.input().responsibility.id)
        {
            return false;
        }
        let ExecutionMode::Single(group) = p.manifest.input().execution else {
            return false;
        };
        let Ok(Some(creation)) = self.group_creation_at(self.applied, group) else {
            return false;
        };
        let plan = NamespacePlan {
            creation,
            manifest: p.manifest.clone(),
        };
        p.matches(&plan).is_ok()
    }
    pub(super) fn complete_namespace(
        &mut self,
        op: OperationId,
        p: NamespacePublication,
    ) -> DirectoryOutcome {
        if !self.namespace_permitted(&p) {
            return DirectoryOutcome::TransferEvidenceMismatch;
        }
        let generation = p.manifest.input().generation;
        self.manifests
            .insert(p.manifest.input().responsibility, p.manifest);
        self.namespace_publications.insert(p.creation, op);
        DirectoryOutcome::NamespacePublished(generation)
    }
    /// Local applied observation. Authenticate metadata commitment before using
    /// remotely; constructing this value never authorizes a target activation.
    pub fn namespace_publication_at(
        &self,
        required: u64,
        creation: OperationId,
    ) -> Result<Option<NamespacePublicationStatus>, ApplicationError> {
        if !self.namespace_creation {
            return Err(ApplicationError::UnsupportedSchema);
        }
        if required > self.applied {
            return Err(ApplicationError::NotApplied);
        }
        self.namespace_publications
            .get(&creation)
            .map(|op| {
                let h = &self.history[op];
                Ok(NamespacePublicationStatus {
                    operation: *op,
                    index: h.index,
                    publication: NamespacePublication::decode(&h.bytes)?,
                })
            })
            .transpose()
    }
}
