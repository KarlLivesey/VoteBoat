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
//! Local application checkpoint contract. Publication certifies the complete
//! state/boundary pair; it does not authorize log deletion or voter replacement.
use crate::{
    application::{ApplicationError, CheckpointStateMachine},
    contracts::StorageError,
    identity::*,
    log::Bootstrap,
    raft::Raft,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotIdentity {
    pub store: StoreIdentity,
    pub group: GroupIdentity,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotMetadata {
    pub bootstrap: Bootstrap,
    pub index: u64,
    pub term: u64,
    pub application_schema: u64,
}
impl SnapshotMetadata {
    pub fn validate(&self) -> Result<(), StorageError> {
        if self.index == 0 || self.term == 0 || self.application_schema == 0 {
            return Err(StorageError::Rejected("invalid snapshot boundary/schema"));
        }
        // Reuse the authoritative bootstrap validation, including membership.
        let mut empty = std::collections::BTreeMap::new();
        crate::log::apply_batch(
            &mut empty,
            &[crate::log::LogMutation::Create(self.bootstrap.clone())],
            crate::log::LogLimits::default(),
        )
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Snapshot {
    pub metadata: SnapshotMetadata,
    pub application: Vec<u8>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotTicket {
    pub binding: StoreBinding,
    pub group: GroupIdentity,
    pub generation: SnapshotGeneration,
}
/// Data has been sealed and synchronized, but its root is not published yet.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SealedSnapshot {
    pub ticket: SnapshotTicket,
    pub file_bytes: u64,
    /// Named native integrity checksum, not cryptographic provenance.
    pub checksum: u32,
}
/// Exact publication receipt. This is not a log-reclamation permission or a
/// permanently pinned object handle: the next publication supersedes it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotReceipt {
    pub sealed: SealedSnapshot,
    pub metadata: SnapshotMetadata,
}
#[derive(Clone, Copy, Debug)]
pub struct SnapshotLimits {
    pub max_application_bytes: usize,
    pub max_metadata_bytes: usize,
    pub max_chunk_bytes: usize,
}
impl Default for SnapshotLimits {
    fn default() -> Self {
        Self {
            max_application_bytes: 64 * 1024 * 1024,
            max_metadata_bytes: 1024 * 1024,
            max_chunk_bytes: 64 * 1024,
        }
    }
}
impl SnapshotLimits {
    pub fn validate(self) -> Result<Self, StorageError> {
        if self.max_application_bytes == 0
            || self.max_application_bytes > 256 * 1024 * 1024
            || self.max_metadata_bytes < 256
            || self.max_metadata_bytes > 4 * 1024 * 1024
            || self.max_chunk_bytes == 0
            || self.max_chunk_bytes > self.max_application_bytes
        {
            return Err(StorageError::Rejected("invalid snapshot limits"));
        }
        Ok(self)
    }
    pub fn max_file_bytes(self) -> usize {
        self.max_application_bytes
            .saturating_add(self.max_metadata_bytes)
            .saturating_add(48)
    }
}

/// One logical snapshot binding per local group. A provider may share physical
/// resources behind several handles. Native storage retains two bounded slots;
/// exactly one stage/seal may be outstanding. Chunks are ordered/offset checked.
/// Seal synchronizes all data; publish atomically makes data+metadata recoverable.
/// Aborting discards the stage, never the published state. Dropping a caller's
/// wait is not rollback. Uncertain I/O fences until explicit recovery.
pub trait SnapshotStore {
    fn identity(&self) -> SnapshotIdentity;
    fn binding(&self) -> StoreBinding;
    fn limits(&self) -> SnapshotLimits;
    fn begin(
        &mut self,
        metadata: SnapshotMetadata,
        application_bytes: usize,
    ) -> Result<SnapshotTicket, StorageError>;
    fn write_chunk(
        &mut self,
        ticket: SnapshotTicket,
        offset: usize,
        bytes: &[u8],
    ) -> Result<(), StorageError>;
    fn seal(&mut self, ticket: SnapshotTicket) -> Result<SealedSnapshot, StorageError>;
    fn publish(&mut self, sealed: SealedSnapshot) -> Result<SnapshotReceipt, StorageError>;
    fn abort(&mut self, ticket: SnapshotTicket) -> Result<(), StorageError>;
    /// Verified latest published image. None means an explicitly created store
    /// has no publication; a missing/corrupt old store is always an error.
    fn load(&mut self) -> Result<Option<Snapshot>, StorageError>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CheckpointError {
    Storage(StorageError),
    Application(ApplicationError),
    InvalidBinding,
    InvalidBoundary,
}
impl From<StorageError> for CheckpointError {
    fn from(e: StorageError) -> Self {
        Self::Storage(e)
    }
}
impl From<ApplicationError> for CheckpointError {
    fn from(e: ApplicationError) -> Self {
        Self::Application(e)
    }
}

fn check_binding<S: SnapshotStore>(raft: &Raft, store: &S) -> Result<(), CheckpointError> {
    if store.identity()
        != (SnapshotIdentity {
            store: raft.storage_binding().identity,
            group: raft.state().bootstrap.group,
        })
        || store.binding().identity != store.identity().store
    {
        return Err(CheckpointError::InvalidBinding);
    }
    Ok(())
}

/// Run on the serialized host owner with the application bound to this group.
/// Calls the same snapshot contract for native and host providers.
pub fn checkpoint_application<A: CheckpointStateMachine, S: SnapshotStore>(
    raft: &Raft,
    application: &A,
    store: &mut S,
) -> Result<SnapshotReceipt, CheckpointError> {
    check_binding(raft, store)?;
    let limits = store.limits().validate()?;
    let index = application.applied_index();
    if index == 0 || index > raft.state().commit_index {
        return Err(CheckpointError::InvalidBoundary);
    }
    let metadata = SnapshotMetadata {
        bootstrap: raft.state().bootstrap.clone(),
        index,
        term: raft
            .state()
            .term_at(index)
            .ok_or(CheckpointError::InvalidBoundary)?,
        application_schema: application.schema_version(),
    };
    metadata.validate()?;
    let bytes = application.checkpoint(limits.max_application_bytes)?;
    if bytes.is_empty() || bytes.len() > limits.max_application_bytes {
        return Err(CheckpointError::InvalidBoundary);
    }
    let ticket = store.begin(metadata.clone(), bytes.len())?;
    if ticket.binding != store.binding() || ticket.group != metadata.bootstrap.group {
        return Err(StorageError::StaleTicket.into());
    }
    for (chunk, data) in bytes.chunks(limits.max_chunk_bytes).enumerate() {
        store.write_chunk(ticket, chunk * limits.max_chunk_bytes, data)?;
    }
    let sealed = store.seal(ticket)?;
    if sealed.ticket != ticket {
        return Err(StorageError::StaleTicket.into());
    }
    let receipt = store.publish(sealed)?;
    if receipt.sealed != sealed || receipt.metadata != metadata {
        return Err(StorageError::StaleTicket.into());
    }
    Ok(receipt)
}

pub struct Restored<R> {
    pub checkpoint_index: u64,
    /// Replay results are not new client acknowledgements.
    pub replay_receipts: Vec<R>,
}

/// Restore a fresh application atomically from a verified checkpoint plus the
/// retained committed tail. This does not reset hard state, commit or log history.
pub fn restore_application<A: CheckpointStateMachine, S: SnapshotStore>(
    raft: &Raft,
    application: &mut A,
    store: &mut S,
) -> Result<Restored<A::Receipt>, CheckpointError> {
    check_binding(raft, store)?;
    if application.applied_index() != 0 {
        return Err(CheckpointError::InvalidBoundary);
    }
    let limits = store.limits().validate()?;
    let mut next = application.clone();
    let mut floor = 0;
    if let Some(snapshot) = store.load()? {
        let m = &snapshot.metadata;
        if m.bootstrap != raft.state().bootstrap
            || m.index > raft.state().commit_index
            || raft.state().term_at(m.index) != Some(m.term)
            || m.application_schema != next.schema_version()
            || snapshot.application.len() > limits.max_application_bytes
        {
            return Err(CheckpointError::InvalidBoundary);
        }
        m.validate()?;
        next.restore_checkpoint(m.application_schema, m.index, &snapshot.application)?;
        if next.applied_index() != m.index {
            return Err(CheckpointError::InvalidBoundary);
        }
        floor = m.index;
    }
    let replay_receipts = next.apply_batch(&raft.replay_committed()[floor as usize..])?;
    *application = next;
    Ok(Restored {
        checkpoint_index: floor,
        replay_receipts,
    })
}
