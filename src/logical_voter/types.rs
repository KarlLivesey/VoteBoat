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
//! Distinct logical/physical identities and bounded original protocol data.
use super::LogicalEnrollmentView;
use crate::{
    application::ApplicationError,
    identity::*,
    log::*,
    membership::Membership,
    runtime::{ClientTicket, EffectTicket, ProposalPosition, ReadInvocationTicket},
    secure::PeerIdentity,
    snapshot::*,
};
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalStoreMapping {
    pub parent: GroupIdentity,
    pub voter: NodeId,
    pub store: StoreIdentity,
    pub child: GroupIdentity,
}
impl LogicalStoreMapping {
    pub fn new(
        parent: GroupIdentity,
        voter: NodeId,
        store: StoreIdentity,
        child: GroupIdentity,
    ) -> Result<Self, ApplicationError> {
        if parent.id == child.id {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(Self {
            parent,
            voter,
            store,
            child,
        })
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RepresentativeEpoch(std::num::NonZeroU64);
impl RepresentativeEpoch {
    pub const fn new(value: u64) -> Option<Self> {
        match std::num::NonZeroU64::new(value) {
            Some(v) => Some(Self(v)),
            None => None,
        }
    }
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RepresentativeBinding {
    pub epoch: RepresentativeEpoch,
    pub physical: PeerIdentity,
    pub physical_session: StoreSession,
    pub logical_session: StoreSession,
}
#[derive(Clone, Copy, Debug)]
pub struct LogicalLimits {
    pub parent_log: LogLimits,
    pub parent_snapshots: SnapshotLimits,
    pub max_command_bytes: usize,
    pub max_actions: usize,
    pub max_retained_action_bytes: usize,
    pub control_actions: usize,
    pub control_action_bytes: usize,
    pub max_capsule_bytes: usize,
    /// Parent checkpoint and nested image/snapshot read allocation envelope.
    pub max_parent_checkpoint_bytes: usize,
    pub max_child_checkpoint_bytes: usize,
}
impl Default for LogicalLimits {
    fn default() -> Self {
        Self {
            parent_log: LogLimits {
                max_groups: 1,
                max_entries_per_group: 256,
                max_command_bytes: 4096,
                max_batch_bytes: 64 * 1024,
                max_batch_units: 1,
                max_pending_units: 8,
                max_snapshot_bytes: 256 * 1024,
                ..LogLimits::default()
            },
            parent_snapshots: SnapshotLimits {
                max_application_bytes: 64 * 1024,
                max_metadata_bytes: 64 * 1024,
                max_chunk_bytes: 4096,
            },
            max_command_bytes: 256 * 1024,
            max_actions: 4096,
            max_retained_action_bytes: 8 * 1024 * 1024,
            control_actions: 128,
            control_action_bytes: 512 * 1024,
            max_capsule_bytes: 128 * 1024,
            max_parent_checkpoint_bytes: 2 * 1024 * 1024,
            max_child_checkpoint_bytes: 16 * 1024 * 1024,
        }
    }
}
impl LogicalLimits {
    pub fn validate(self) -> Result<Self, ApplicationError> {
        self.parent_log
            .validate()
            .map_err(|_| ApplicationError::InvalidCommand)?;
        self.parent_snapshots
            .validate()
            .map_err(|_| ApplicationError::InvalidCommand)?;
        if self.parent_log.max_groups != 1
            || self.max_actions < 2
            || self.max_actions > 65536
            || self.control_actions == 0
            || self.control_actions >= self.max_actions
            || self.max_command_bytes < 512
            || self.max_command_bytes > 16 * 1024 * 1024
            || self.max_retained_action_bytes < self.max_command_bytes
            // Checkpoints retain every original action command. This necessary
            // conservation check does not bound metadata or configuration growth.
            || self.max_retained_action_bytes > self.max_child_checkpoint_bytes
            || self.control_action_bytes == 0
            || self.control_action_bytes >= self.max_retained_action_bytes
            || self.max_capsule_bytes == 0
            || self.max_capsule_bytes > self.max_command_bytes
            || self.parent_log.max_snapshot_bytes < self.parent_snapshots.max_file_bytes()
            || self.max_parent_checkpoint_bytes > self.parent_log.max_wal_bytes
            || self.max_parent_checkpoint_bytes > self.max_child_checkpoint_bytes
            || self.max_parent_checkpoint_bytes == 0
            || self.max_child_checkpoint_bytes == 0
            || self.max_child_checkpoint_bytes > 256 * 1024 * 1024
        {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(self)
    }
}
#[cfg(test)]
#[path = "types/capacity.rs"]
mod capacity;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalRefusal {
    StaleRepresentative,
    StaleImage,
    Ineligible,
    MissingSnapshot,
    SnapshotConflict,
    InvalidResponse,
    OperationConflict,
    /// The operation ID is below the child's retirement watermark: its
    /// session is closed or its outcome was acknowledged as consumed. It is
    /// never executed again; a committed copy carries `duplicate: true`.
    Expired,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalOutcome {
    Claimed,
    Mutated,
    SnapshotPublished(SnapshotRef),
    SnapshotReconciled,
    CapsuleAuthorized,
    Refused(LogicalRefusal),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalReceipt {
    pub index: u64,
    pub operation: OperationId,
    pub first_index: u64,
    pub outcome: LogicalOutcome,
    pub parent_revision: Option<LogRevision>,
    pub parent_generation: Option<LogGeneration>,
    pub epoch: Option<RepresentativeEpoch>,
    pub duplicate: bool,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalActionStatus {
    pub operation: OperationId,
    pub first_index: u64,
    pub first_term: u64,
    pub preceding_configuration: ConfigurationId,
    pub command: Vec<u8>,
    pub receipt: LogicalReceipt,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalImage {
    pub mapping: LogicalStoreMapping,
    pub representative: RepresentativeBinding,
    pub parent: GroupLog,
    pub child_membership: Membership,
    pub child_applied: u64,
    pub published: Option<SnapshotRef>,
    pub pins: Vec<SnapshotRef>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalMetadata {
    pub mapping: LogicalStoreMapping,
    pub representative: Option<RepresentativeBinding>,
    pub child_applied: u64,
    pub retained_actions: usize,
    pub retained_action_bytes: usize,
    pub parent_revision: Option<LogRevision>,
    pub parent_generation: Option<LogGeneration>,
    /// Session-space retirement watermark: every session-space operation ID
    /// below it is `Expired`.
    pub retired_below: u128,
}
/// Outcome lookup of one operation ID. `Unknown` is never proof that the
/// operation did not commit; `Expired` is never proof that it did.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogicalOutcomeLookup {
    /// The original record with its first receipt.
    Retained(Box<LogicalActionStatus>),
    /// A session-space ID below the watermark: never executed again.
    Expired,
    /// Not retained and not retired: pending, never proposed, or a pruned
    /// claim whose later copies can only refuse.
    Unknown,
}
/// Original applied configuration entries; inspection data grants no authority.
/// Empty entries mean unknown at `child_applied`, not a terminal refusal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalConfigurationOperation {
    pub child: GroupIdentity,
    pub operation: OperationId,
    pub child_applied: u64,
    pub entries: Vec<LogEntry>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalQuery {
    Image,
    Outcome(OperationId),
    Snapshot(SnapshotRef),
    Metadata,
    ConfigurationOperation(OperationId),
    Enrollment,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogicalRead {
    Image(Box<LogicalImage>),
    Outcome(LogicalOutcomeLookup),
    Snapshot(Box<Snapshot>),
    Metadata(Box<LogicalMetadata>),
    Uninitialized,
    ConfigurationOperation(Box<LogicalConfigurationOperation>),
    Enrollment(Box<LogicalEnrollmentView>),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PersistOrigin {
    pub request: crate::worker::WorkerTicket,
    pub visit: crate::runtime::VisitTicket,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotOrigin {
    pub request: crate::snapshot_worker::SnapshotWorkTicket,
    pub visit: crate::runtime::VisitTicket,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalReadContext {
    pub group: GroupIdentity,
    pub configuration: ConfigurationId,
    pub term: u64,
    pub request: ReadRequestId,
    pub index: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalCapsuleOrigin {
    Protocol(EffectTicket),
    Client {
        effect: EffectTicket,
        ticket: LogicalClientTicket,
        position: ProposalPosition,
    },
    Read {
        effect: EffectTicket,
        ticket: LogicalReadTicket,
        barrier: LogicalReadContext,
    },
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogicalCapsuleBody {
    Protocol(crate::raft::Message),
    ClientApplied {
        command: Vec<u8>,
        schema: u64,
        receipt: Vec<u8>,
    },
    ReadResult {
        schema: u64,
        query: Vec<u8>,
        result: Vec<u8>,
    },
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalCapsule {
    pub expected_revision: LogRevision,
    pub expected_generation: LogGeneration,
    pub local_configuration: ConfigurationId,
    pub origin: LogicalCapsuleOrigin,
    pub body: LogicalCapsuleBody,
}

/// Persistent original-client correlation data; it contains no runtime authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalClientTicket {
    pub owner: crate::runtime::RuntimeOwner,
    pub generation: ClientRouterGeneration,
    pub sequence: u64,
    pub group: GroupIdentity,
    pub operation: OperationId,
}
impl From<ClientTicket> for LogicalClientTicket {
    fn from(t: ClientTicket) -> Self {
        Self {
            owner: t.binding.owner,
            generation: t.binding.generation,
            sequence: t.sequence,
            group: t.group,
            operation: t.operation,
        }
    }
}
/// Persistent original-read correlation data; it cannot create a read barrier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalReadTicket {
    pub owner: crate::runtime::RuntimeOwner,
    pub generation: ReadInvocationGeneration,
    pub sequence: u64,
    pub group: GroupIdentity,
    pub request: ReadRequestId,
}
impl From<ReadInvocationTicket> for LogicalReadTicket {
    fn from(t: ReadInvocationTicket) -> Self {
        Self {
            owner: t.binding.owner,
            generation: t.binding.generation,
            sequence: t.sequence,
            group: t.group,
            request: t.request,
        }
    }
}
