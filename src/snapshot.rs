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
    log::{Bootstrap, LogStore},
    raft::{persist_effect, Effect, Message, Raft, RaftError, RecoveryMode, RequestContext, Rpc},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotIdentity {
    pub store: StoreIdentity,
    pub group: GroupIdentity,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotMetadata {
    pub bootstrap: Bootstrap,
    pub membership: Option<Box<crate::membership::Membership>>,
    pub index: u64,
    pub term: u64,
    pub application_schema: u64,
}
impl SnapshotMetadata {
    pub fn configuration(&self) -> ConfigurationId {
        self.membership
            .as_ref()
            .map_or(self.bootstrap.configuration, |m| m.id())
    }
    /// A later local checkpoint cannot rewrite its already published prefix.
    /// Log installation still verifies the exact matching configuration base.
    pub fn follows(&self, previous: &Self) -> bool {
        if self.bootstrap != previous.bootstrap
            || self.index <= previous.index
            || self.term < previous.term
            || self.application_schema != previous.application_schema
            || self.configuration() < previous.configuration()
        {
            return false;
        }
        if self.configuration() == previous.configuration() {
            return self.membership == previous.membership;
        }
        let Some(next) = &self.membership else {
            return false;
        };
        next.last_configuration_index() > previous.index
            && previous
                .membership
                .as_ref()
                .is_none_or(|old| next.operations().is_superset(old.operations()))
    }
    pub fn validate(&self) -> Result<(), StorageError> {
        if self.index == 0
            || self.index == u64::MAX
            || self.term == 0
            || self.application_schema == 0
        {
            return Err(StorageError::Rejected("invalid snapshot boundary/schema"));
        }
        // Reuse the authoritative bootstrap validation, including membership.
        let mut empty = std::collections::BTreeMap::new();
        crate::log::apply_batch(
            &mut empty,
            &[crate::log::LogMutation::Create(self.bootstrap.clone())],
            crate::log::LogLimits::default(),
        )?;
        if let Some(membership) = &self.membership {
            membership
                .validate_checkpoint(&self.bootstrap, self.index)
                .map_err(|_| StorageError::Rejected("invalid snapshot membership"))?;
        }
        Ok(())
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
/// Stable persisted reference; sessions scope admissions, not durable roots.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotRef {
    pub store: StoreIdentity,
    pub group: GroupIdentity,
    pub generation: SnapshotGeneration,
    pub configuration: ConfigurationId,
    pub index: u64,
    pub term: u64,
    pub application_schema: u64,
    pub file_bytes: u64,
    pub checksum: u32,
}
impl SnapshotReceipt {
    pub fn reference(&self) -> SnapshotRef {
        SnapshotRef {
            store: self.sealed.ticket.binding.identity,
            group: self.metadata.bootstrap.group,
            generation: self.sealed.ticket.generation,
            configuration: self.metadata.configuration(),
            index: self.metadata.index,
            term: self.metadata.term,
            application_schema: self.metadata.application_schema,
            file_bytes: self.sealed.file_bytes,
            checksum: self.sealed.checksum,
        }
    }
}
impl SnapshotRef {
    pub fn matches(&self, snapshot: &Snapshot) -> bool {
        self.group == snapshot.metadata.bootstrap.group
            && self.configuration == snapshot.metadata.configuration()
            && self.index == snapshot.metadata.index
            && self.term == snapshot.metadata.term
            && self.application_schema == snapshot.metadata.application_schema
    }
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

/// Durable log anchors, distinct from temporary transport-buffer ownership.
/// Pin before recording a log boundary. Release only after a verified durable
/// log no longer references it. At most two anchors cover one in-flight switch.
/// A published image may be replaced; an anchored image must not be overwritten.
pub trait SnapshotRetention: SnapshotStore {
    fn latest_reference(&self) -> Result<Option<SnapshotRef>, StorageError>;
    fn pin_for_log(&mut self, reference: SnapshotRef) -> Result<(), StorageError>;
    fn load_pinned(&mut self, reference: SnapshotRef) -> Result<Snapshot, StorageError>;
    /// Caller supplies the one authoritative durable log reference. Select it
    /// as root and release all other log anchors; owned transport images are separate.
    fn reconcile_log(&mut self, reference: Option<SnapshotRef>) -> Result<(), StorageError>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CheckpointError {
    Storage(StorageError),
    Application(ApplicationError),
    InvalidBinding,
    InvalidBoundary,
    Consensus(RaftError),
}
impl From<RaftError> for CheckpointError {
    fn from(e: RaftError) -> Self {
        Self::Consensus(e)
    }
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
        membership: raft
            .state()
            .checkpoint_membership(index)
            .map_err(|_| CheckpointError::InvalidBoundary)?,
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
pub fn restore_application<A: CheckpointStateMachine, S: SnapshotRetention>(
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
    let snapshot = if let Some(reference) = raft.state().snapshot {
        Some(store.load_pinned(reference)?)
    } else {
        store.load()?
    };
    if let Some(snapshot) = snapshot {
        let m = &snapshot.metadata;
        if m.bootstrap != raft.state().bootstrap
            || m.index > raft.state().commit_index
            || raft.state().term_at(m.index) != Some(m.term)
            || m.application_schema != next.schema_version()
            || snapshot.application.len() > limits.max_application_bytes
            || raft.state().snapshot.is_some_and(|r| !r.matches(&snapshot))
        {
            return Err(CheckpointError::InvalidBoundary);
        }
        m.validate()?;
        if raft
            .state()
            .checkpoint_membership(m.index)
            .map_err(|_| CheckpointError::InvalidBoundary)?
            != m.membership
        {
            return Err(CheckpointError::InvalidBoundary);
        }
        next.restore_checkpoint(m.application_schema, m.index, &snapshot.application)?;
        if next.applied_index() != m.index {
            return Err(CheckpointError::InvalidBoundary);
        }
        floor = m.index;
    }
    if floor < raft.state().base_index() {
        return Err(CheckpointError::InvalidBoundary);
    }
    let replay_receipts =
        next.apply_batch(&raft.replay_committed()[(floor - raft.state().base_index()) as usize..])?;
    *application = next;
    Ok(Restored {
        checkpoint_index: floor,
        replay_receipts,
    })
}

fn verify_log<L: LogStore>(raft: &Raft, log: &L) -> Result<(), CheckpointError> {
    if log.binding() != raft.storage_binding()
        || log.state(raft.state().bootstrap.group)? != *raft.state()
    {
        return Err(CheckpointError::InvalidBinding);
    }
    Ok(())
}
/// Recovery of a compacted voter verifies retained data and restores application
/// before returning a core that can participate in elections or acknowledge data.
pub fn recover_replica<A: CheckpointStateMachine, L: LogStore, S: SnapshotRetention>(
    node: NodeId,
    group: GroupIdentity,
    log: &L,
    store: &mut S,
    application: &mut A,
) -> Result<(Raft, Restored<A::Receipt>), CheckpointError> {
    recover_replica_as(
        node,
        group,
        log,
        store,
        application,
        RecoveryMode::StaticVoter,
    )
}
/// Explicit host-authorized learner recovery verifies the committed exact-store
/// assignment, pinned application data and replay before exposing a core. This
/// does not authorize promotion or bypass the online reconfiguration gate.
pub fn recover_learner_replica<A: CheckpointStateMachine, L: LogStore, S: SnapshotRetention>(
    node: NodeId,
    group: GroupIdentity,
    log: &L,
    store: &mut S,
    application: &mut A,
) -> Result<(Raft, Restored<A::Receipt>), CheckpointError> {
    recover_replica_as(
        node,
        group,
        log,
        store,
        application,
        RecoveryMode::BootstrapLearner,
    )
}
/// Explicit host-authorized dynamic member recovery. Exact committed/accepted
/// assignment, pinned checkpoint verification and committed replay precede
/// returning the core. Accepted membership determines voting eligibility.
pub fn recover_member_replica<A: CheckpointStateMachine, L: LogStore, S: SnapshotRetention>(
    node: NodeId,
    group: GroupIdentity,
    log: &L,
    store: &mut S,
    application: &mut A,
) -> Result<(Raft, Restored<A::Receipt>), CheckpointError> {
    recover_replica_as(node, group, log, store, application, RecoveryMode::Member)
}
fn recover_replica_as<A: CheckpointStateMachine, L: LogStore, S: SnapshotRetention>(
    node: NodeId,
    group: GroupIdentity,
    log: &L,
    store: &mut S,
    application: &mut A,
    mode: RecoveryMode,
) -> Result<(Raft, Restored<A::Receipt>), CheckpointError> {
    let state = log.state(group)?;
    let core = match mode {
        RecoveryMode::BootstrapLearner => {
            Raft::recover_learner_verified(node, log.binding(), state, log.limits())?
        }
        RecoveryMode::StaticVoter => {
            Raft::recover_verified(node, log.binding(), state, log.limits())?
        }
        RecoveryMode::Member => {
            Raft::recover_member_verified(node, log.binding(), state, log.limits())?
        }
    };
    check_binding(&core, store)?;
    if core.state().snapshot.is_none() {
        if let Some(snapshot) = store.load()? {
            if snapshot.metadata.index > core.state().commit_index {
                if snapshot.metadata.bootstrap != core.state().bootstrap {
                    return Err(CheckpointError::InvalidBoundary);
                }
                let mut check = application.clone();
                check.restore_checkpoint(
                    snapshot.metadata.application_schema,
                    snapshot.metadata.index,
                    &snapshot.application,
                )?;
                store.reconcile_log(None)?;
            }
        }
    }
    let restored = restore_application(&core, application, store)?;
    store.reconcile_log(core.state().snapshot)?;
    Ok((core, restored))
}
pub fn compact_replica<A: CheckpointStateMachine, L: LogStore, S: SnapshotRetention>(
    raft: &mut Raft,
    log: &mut L,
    store: &mut S,
    application: &A,
    reference: SnapshotRef,
) -> Result<Vec<Effect>, CheckpointError> {
    verify_log(raft, log)?;
    check_binding(raft, store)?;
    if application.applied_index() < reference.index {
        return Err(CheckpointError::InvalidBoundary);
    }
    store.pin_for_log(reference)?;
    let snapshot = store.load_pinned(reference)?;
    if !reference.matches(&snapshot)
        || snapshot.metadata.bootstrap != raft.state().bootstrap
        || snapshot.metadata.membership
            != raft
                .state()
                .checkpoint_membership(reference.index)
                .map_err(|_| CheckpointError::InvalidBoundary)?
    {
        return Err(CheckpointError::InvalidBoundary);
    }
    let mut check = application.clone();
    check.restore_checkpoint(
        reference.application_schema,
        reference.index,
        &snapshot.application,
    )?;
    let effects = raft.begin_compact(reference)?;
    let [Effect::Persist(update)] = effects.as_slice() else {
        return Err(CheckpointError::InvalidBoundary);
    };
    let effects = persist_effect(raft, log, update.clone())?;
    if let Err(e) = store.reconcile_log(Some(reference)) {
        raft.storage_failed();
        return Err(e.into());
    }
    Ok(effects)
}
/// A snapshot message owns its bounded image; no pin is released while a read
/// is outstanding. A later compaction invalidates the old request context.
pub fn supply_snapshot<S: SnapshotRetention>(
    raft: &Raft,
    store: &mut S,
    to: NodeId,
    context: RequestContext,
    reference: SnapshotRef,
) -> Result<Vec<Effect>, CheckpointError> {
    check_binding(raft, store)?;
    let snapshot = store.load_pinned(reference)?;
    Ok(raft.snapshot_send(to, context, reference, snapshot)?)
}
/// Selected snapshot and log providers certify separate dependencies. No remote
/// acknowledgement escapes until pin, log binding and application restore finish.
pub fn stage_snapshot_effect<A: CheckpointStateMachine, L: LogStore, S: SnapshotRetention>(
    raft: &mut Raft,
    log: &mut L,
    store: &mut S,
    application: &A,
    message: Message,
) -> Result<Vec<Effect>, CheckpointError> {
    let result = (|| {
        verify_log(raft, log)?;
        check_binding(raft, store)?;
        if !raft.staged_matches(&message) {
            return Err(CheckpointError::Consensus(RaftError::WrongCompletion));
        }
        let Rpc::Snapshot { snapshot } = &message.rpc else {
            return Err(CheckpointError::InvalidBoundary);
        };
        let mut check = application.clone();
        check.restore_checkpoint(
            snapshot.metadata.application_schema,
            snapshot.metadata.index,
            &snapshot.application,
        )?;
        store.reconcile_log(raft.state().snapshot)?;
        let ticket = store.begin(snapshot.metadata.clone(), snapshot.application.len())?;
        if ticket.binding != store.binding() || ticket.group != snapshot.metadata.bootstrap.group {
            return Err(StorageError::StaleTicket.into());
        }
        let size = store.limits().validate()?.max_chunk_bytes;
        for (i, chunk) in snapshot.application.chunks(size).enumerate() {
            store.write_chunk(ticket, i * size, chunk)?;
        }
        let sealed = store.seal(ticket)?;
        if sealed.ticket != ticket {
            return Err(StorageError::StaleTicket.into());
        }
        let receipt = store.publish(sealed)?;
        if receipt.sealed != sealed || receipt.metadata != snapshot.metadata {
            return Err(StorageError::StaleTicket.into());
        }
        let reference = receipt.reference();
        store.pin_for_log(reference)?;
        if store.load_pinned(reference)? != **snapshot {
            return Err(CheckpointError::InvalidBoundary);
        }
        let effects = raft.snapshot_stored(reference)?;
        let [Effect::Persist(update)] = effects.as_slice() else {
            return Err(CheckpointError::InvalidBoundary);
        };
        Ok(persist_effect(raft, log, update.clone())?)
    })();
    if result.is_err() {
        raft.storage_failed();
    }
    result
}
pub fn finish_snapshot_install<A: CheckpointStateMachine, L: LogStore, S: SnapshotRetention>(
    raft: &mut Raft,
    log: &L,
    store: &mut S,
    application: &mut A,
    reference: SnapshotRef,
) -> Result<Vec<Effect>, CheckpointError> {
    let result = (|| {
        verify_log(raft, log)?;
        check_binding(raft, store)?;
        if raft.state().snapshot != Some(reference) || application.applied_index() > reference.index
        {
            return Err(CheckpointError::InvalidBoundary);
        }
        let snapshot = store.load_pinned(reference)?;
        if !reference.matches(&snapshot)
            || snapshot.metadata.bootstrap != raft.state().bootstrap
            || snapshot.metadata.membership != raft.state().snapshot_membership
        {
            return Err(CheckpointError::InvalidBoundary);
        }
        let mut next = application.clone();
        next.restore_checkpoint(
            reference.application_schema,
            reference.index,
            &snapshot.application,
        )?;
        next.apply_batch(raft.replay_committed())?;
        store.reconcile_log(Some(reference))?;
        let effects = raft.snapshot_applied(reference, next.applied_index())?;
        *application = next;
        Ok(effects)
    })();
    if result.is_err() {
        raft.storage_failed();
    }
    result
}
