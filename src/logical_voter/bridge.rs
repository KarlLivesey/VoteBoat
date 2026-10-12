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
//! One explicit bounded consumer of real child Node client and quorum-read streams.
use super::*;
use crate::{
    contracts::StorageError,
    log_format::LogCodec,
    outbound::OutboundQueue,
    raft::{RaftError, ReadBarrier, Role},
    runtime::*,
    snapshot_format::SnapshotCodec,
    snapshot_worker::SnapshotWorker,
    wire::WireCodec,
    worker::PersistenceWorker,
};
pub(crate) mod activation;
pub use activation::{LogicalActivationAttachmentRejected, LogicalActivationContinuation};
pub(crate) mod backing;
mod construction;
mod consume;
mod enrollment;
pub use enrollment::{LogicalEnrollmentRecovery, LogicalEnrollmentRecoveryRejected};
mod persistence;
pub(crate) mod polling;
mod release;
mod snapshots;
pub use backing::*;
/// The original physical Node spelling remains available to existing embeddings.
pub type LogicalChildBridge<'a, S, T, E, L, V, B, W, O, H, C, F> =
    LogicalBackingBridge<'a, Node<S, T, E, LogicalApplication<L, V, B>, W, O, H, C, F>>;
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogicalBridgeError {
    InvalidAssembly,
    Busy,
    Closed,
    Exhausted,
    TimeWentBack,
    ProviderViolation,
    StaleRepresentative,
    Refused(LogicalRefusal),
    Application(ApplicationError),
    Storage(StorageError),
    Endpoint(LogicalEndpointError),
    Node {
        node: usize,
        reason: Box<NodeError>,
    },
    Backing {
        node: usize,
        reason: Box<LogicalBackingPollError>,
    },
    Client {
        node: usize,
        reason: Box<ClientError>,
    },
    Read {
        node: usize,
        reason: Box<ReadInvocationError>,
    },
    Release(LogicalReleaseError),
}
#[derive(Clone, Copy, Debug, Default)]
pub struct LogicalBridgeBudget<P = NodePollBudget> {
    pub node_polls: usize,
    pub completions: usize,
    pub transitions: usize,
    pub node: P,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LogicalBridgeUsage {
    pub actions: usize,
    pub command_bytes: usize,
    pub child_attempts: usize,
    pub retained_outputs: usize,
    pub image_ready: bool,
    /// Full owned enrollment command, lineage and recovery-read allocations.
    pub enrollment_bytes: usize,
}
/// At most one original action/read sequence is owned by this bridge. Nodes and
/// their streams are exclusively borrowed; parent workers use explicit host
/// handles, never a synchronous network call or a hidden executor.
pub struct LogicalBackingBridge<'a, N: LogicalBackingChild> {
    nodes: &'a mut [N],
    mapping: LogicalStoreMapping,
    representative_node: usize,
    initialized: Option<RepresentativeBinding>,
    initialized_at: Option<ReadBarrier>,
    activation_verified: bool,
    work: Option<BridgeWork>,
    image: Option<VerifiedLogicalImage>,
    action: Option<CommittedLogicalAction>,
    retained: Option<RetainedChildOutput>,
    failure: Option<LogicalBridgeError>,
    node_cursor: usize,
    output_cursor: usize,
    route_cursor: usize,
    now: Option<MonoTime>,
    closed: bool,
    sequence: u64,
    release_ticket: Option<LogicalReleaseTicket>,
    activation_history: Option<Box<activation::ObservedLogicalConfiguration>>,
    readiness: Option<activation::ReadinessAttempt>,
    configuration_attempt: Option<activation::ConfigurationAttempt>,
    activation_stopping: bool,
}
struct BridgeWork {
    stage: BridgeStage,
    attempt: Option<ChildAttempt>,
    candidate: bool,
    stopped: bool,
    cancellation_requested: bool,
    enrollment: Option<Box<startup::ObservedLogicalEnrollment>>,
    original_claim: Option<(Box<Command>, Vec<u8>)>,
}
impl BridgeWork {
    fn new(stage: BridgeStage, candidate: bool) -> Self {
        Self {
            stage,
            attempt: None,
            candidate,
            stopped: false,
            cancellation_requested: false,
            enrollment: None,
            original_claim: None,
        }
    }
}
enum BridgeStage {
    BeforeClaim(OperationId),
    BeforeEnrollment {
        operation: OperationId,
        enrollment: Box<LogicalLearnerEnrollment>,
    },
    Enrollment {
        claim: Box<LogicalReceipt>,
        representative: RepresentativeBinding,
    },
    ActivationHistory(OperationId),
    ActivationRepresentative(OperationId),
    Applying {
        command: Box<Command>,
        bytes: Vec<u8>,
    },
    Resolving {
        command: Box<Command>,
        bytes: Vec<u8>,
    },
    AfterClaim {
        claim: Box<LogicalReceipt>,
        representative: RepresentativeBinding,
    },
    Snapshots(Box<SnapshotReads>),
}
struct SnapshotReads {
    image: LogicalImage,
    claim: LogicalReceipt,
    barrier: ReadBarrier,
    references: Vec<SnapshotRef>,
    snapshots: Vec<(SnapshotRef, Snapshot)>,
    enrollment: Option<Box<startup::ObservedLogicalEnrollment>>,
}
#[derive(Clone, Copy)]
enum ChildAttempt {
    Client {
        node: usize,
        ticket: ClientTicket,
    },
    Read {
        node: usize,
        ticket: ReadInvocationTicket,
        query: LogicalQuery,
    },
}
enum RetainedChildOutput {
    Client {
        node: usize,
        output: Box<ClientCompletion<LogicalReceipt>>,
    },
    Read {
        node: usize,
        output: Box<ReadCompletion<LogicalRead>>,
    },
}
/// Crate-private consumed-child observation. Public scalar receipts cannot mint it.
pub(crate) struct CommittedLogicalAction {
    mapping: LogicalStoreMapping,
    command: Box<Command>,
    bytes: Vec<u8>,
    receipt: LogicalReceipt,
    child_owner: RuntimeOwner,
}
impl CommittedLogicalAction {
    pub(crate) fn bytes_capacity(&self) -> usize {
        self.bytes.capacity()
    }
    pub(crate) fn command_body(&self) -> &CommandBody {
        &self.command.body
    }
    pub(crate) fn persist(&self) -> Option<(PersistOrigin, &LogUpdate, LogGeneration)> {
        if let CommandBody::Mutate {
            expected_generation,
            update,
            origin,
        } = &self.command.body
        {
            Some((*origin, update, *expected_generation))
        } else {
            None
        }
    }
    pub(crate) fn mapping(&self) -> LogicalStoreMapping {
        self.mapping
    }
    pub(crate) fn representative(&self) -> RepresentativeBinding {
        self.command.representative
    }
    pub(crate) fn receipt(&self) -> LogicalReceipt {
        self.receipt
    }
    pub(crate) fn capsule(&self) -> Option<&LogicalCapsule> {
        if let CommandBody::Authorize(capsule) = &self.command.body {
            Some(capsule)
        } else {
            None
        }
    }
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub(crate) fn child_owner(&self) -> RuntimeOwner {
        self.child_owner
    }
}
