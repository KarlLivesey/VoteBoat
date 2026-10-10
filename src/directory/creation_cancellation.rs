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

pub const GROUP_CREATION_CANCELLATION_BYTES: usize = 56;

/// Administrative request against an exact retained creation. Constructing this
/// value supplies neither authorization nor evidence of committed cancellation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CancelGroupCreation {
    pub creation: OperationId,
    pub creation_index: u64,
    pub group: GroupIdentity,
}

impl CancelGroupCreation {
    pub fn from_status(status: &GroupCreationStatus) -> Self {
        Self {
            creation: status.operation,
            creation_index: status.index,
            group: status.intent.bootstrap.group,
        }
    }

    pub fn encode(&self, max_bytes: usize) -> Result<Vec<u8>, ApplicationError> {
        if self.creation_index == 0 || max_bytes < GROUP_CREATION_CANCELLATION_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut bytes = Vec::with_capacity(GROUP_CREATION_CANCELLATION_BYTES);
        bytes.extend(b"VBGCAN01");
        bytes.extend(self.creation.get().to_le_bytes());
        bytes.extend(self.creation_index.to_le_bytes());
        put_group(&mut bytes, self.group);
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ApplicationError> {
        if bytes.len() != GROUP_CREATION_CANCELLATION_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBGCAN01" {
            return Err(ApplicationError::InvalidCommand);
        }
        let value = Self {
            creation: r.operation()?,
            creation_index: r.u64()?,
            group: r.group()?,
        };
        if value.creation_index == 0 || !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(value)
    }
}

/// Local applied observation; remote use requires authenticated quorum-read
/// provenance. Cancellation never grants file reclamation or owner revocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupCreationCancellationStatus {
    pub operation: OperationId,
    pub index: u64,
    pub cancellation: CancelGroupCreation,
}

impl Directory {
    pub(super) fn creation_cancelled(&self, group: GroupIdentity) -> bool {
        // Transfers must not recycle a canceled ID by changing its incarnation.
        self.creations
            .iter()
            .any(|(g, (_, op))| g.id == group.id && self.creation_cancellations.contains_key(op))
    }

    pub(super) fn creation_cancellation_permitted(&self, c: CancelGroupCreation) -> bool {
        self.creation_cancellation
            && !self.namespace_publications.contains_key(&c.creation)
            && !self.insertion_creations.contains(&c.creation)
            && !self.transfer_target_busy(c.group)
            && self
                .group_creation_at(self.applied, c.group)
                .ok()
                .flatten()
                .is_some_and(|s| s.operation == c.creation && s.index == c.creation_index)
    }

    pub(super) fn cancel_creation(
        &mut self,
        operation: OperationId,
        c: CancelGroupCreation,
    ) -> DirectoryOutcome {
        if !self.creation_cancellation_permitted(c) {
            return DirectoryOutcome::TransferEvidenceMismatch;
        }
        self.creation_cancellations.insert(c.creation, operation);
        DirectoryOutcome::CreationCancelled
    }

    /// Resolve a lost cancellation receipt using the original creation operation.
    /// `required` is a minimum applied prefix, not a historical snapshot selector.
    pub fn group_creation_cancellation_at(
        &self,
        required: u64,
        creation: OperationId,
    ) -> Result<Option<GroupCreationCancellationStatus>, ApplicationError> {
        if !self.creation_cancellation {
            return Err(ApplicationError::UnsupportedSchema);
        }
        if required > self.applied {
            return Err(ApplicationError::NotApplied);
        }
        self.creation_cancellations
            .get(&creation)
            .map(|op| {
                let h = &self.history[op];
                Ok(GroupCreationCancellationStatus {
                    operation: *op,
                    index: h.index,
                    cancellation: CancelGroupCreation::decode(&h.bytes)?,
                })
            })
            .transpose()
    }
}
