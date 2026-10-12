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
//! Owning logical embedding with explicit child-backed progress and network ownership.
use super::*;
use crate::{
    outbound::*,
    raft::{Event, Message},
    runtime::*,
    secure::SecureSession,
    snapshot_worker::SnapshotWorker,
    transport::{TransportPollBudget, TransportState},
    worker::PersistenceWorker,
};
use std::collections::VecDeque;
mod activation;
mod administration;
mod backing;
mod construction;
mod exports;
mod lifecycle;
mod network;
mod peers;
pub use peers::LogicalPeerAttachRejected;
mod requests;
mod shutdown;

/// Original selected local providers; there is no physical parent WAL.
pub type LogicalNodeLocal<S, T, E, A> = NodeLocalParts<
    S,
    T,
    E,
    A,
    LogicalPersistenceWorker,
    LogicalNetworkQueue,
    LogicalSnapshotWorker,
>;
/// Every resource is supplied by the host and returned intact on rejection.
pub struct LogicalNodeParts<
    S: ReadyScheduler,
    T: TimerService,
    E: ElectionEntropy,
    A: StateMachine + ReadableStateMachine,
    U: SecureSession = Box<dyn SecureSession>,
    C = NativeLogCodec,
    V = NativeSnapshotCodec,
    B = NativeWireCodec,
> where
    A::Receipt: ApplicationReceipt,
{
    pub local: LogicalNodeLocal<S, T, E, A>,
    pub persistence: LogicalPersistencePort,
    pub snapshots: Option<LogicalSnapshotPort>,
    pub sessions: BTreeMap<NodeId, LogicalPeerSession<U, C, V, B>>,
}
#[derive(Clone, Copy, Debug, Default)]
pub struct LogicalNodeLimits {
    pub replica: ReplicaDriverLimits,
    pub releases: LogicalReleaseLimits,
    pub configuration: ConfigurationRequestLimits,
}
#[derive(Clone, Copy, Debug)]
pub struct LogicalNodePollBudget {
    pub replica: ReplicaPollBudget,
    pub sessions: usize,
    pub dispatch: usize,
    pub transport: TransportPollBudget,
}
impl Default for LogicalNodePollBudget {
    fn default() -> Self {
        Self {
            replica: ReplicaPollBudget::default(),
            sessions: 16,
            dispatch: 16,
            transport: TransportPollBudget::default(),
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogicalNodeError {
    InvalidLimits,
    TimeWentBack,
    Closed,
    RecoveryRequired,
    WrongBinding,
    MissingPeers,
    NotQuiescent,
    Endpoint(LogicalEndpointError),
    Replica(ReplicaError),
    Network(LogicalNetworkError),
    Owner(EffectOwnerError),
    Configuration(ConfigurationRequestError),
}
pub struct LogicalNodeRejected<P> {
    pub reason: LogicalNodeError,
    pub parts: Box<P>,
}
/// Local delivery observations from one bounded explicit shutdown poll.
/// Failed and Cancelled do not establish whether a remote replica applied work.
#[derive(Debug, Default)]
pub struct LogicalNodeShutdownProgress {
    pub replica: ReplicaProgress,
    pub completions: Vec<LocalSendCompletion>,
    pub peer_closures: Vec<LogicalPeerClosure>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalPeerClosure {
    pub peer: NodeId,
    pub reason: LogicalNetworkError,
}
/// Previously collected local observations survive a later failure in the poll.
#[derive(Debug)]
pub struct LogicalNodeShutdownRejected {
    pub reason: LogicalNodeError,
    pub progress: Box<LogicalNodeShutdownProgress>,
}
pub type LogicalNodeBuild<N, P> = Result<N, LogicalNodeRejected<P>>;
pub type LogicalNodeConstruct<S, T, E, A, U, C, V, B> =
    LogicalNodeBuild<LogicalNode<S, T, E, A, U, C, V, B>, LogicalNodeParts<S, T, E, A, U, C, V, B>>;
pub type LogicalNodeReclaim<S, T, E, A, U, C, V, B> =
    Result<LogicalNodeParts<S, T, E, A, U, C, V, B>, Box<LogicalNode<S, T, E, A, U, C, V, B>>>;
pub type LogicalNodeRecover<S, T, E, A, U, C, V, B> = Result<
    LogicalNodeRecoveryParts<S, T, E, A, U, C, V, B>,
    Box<LogicalNode<S, T, E, A, U, C, V, B>>,
>;
pub struct LogicalNodePump<'a, R: ApplicationReceipt, V> {
    pub persistence: &'a mut LogicalPersistencePort,
    pub releases: &'a mut LogicalReleaseCoordinator<R, V>,
    pub snapshots: Option<&'a mut LogicalSnapshotPort>,
}
/// Accepted work and original rejected outputs accompany their selected providers.
pub struct LogicalNodeRecovery<P, R: ApplicationReceipt, V> {
    pub parts: P,
    pub replica: LogicalReplicaDriver<R, V>,
    pub configuration: ConfigurationRequests,
    pub pending: VecDeque<LogicalNetworkBatch>,
    /// Connection metadata only; original batches and credits remain in `pending`.
    pub deferred: Option<(NodeId, StoreIdentity)>,
    pub incoming: BTreeMap<NodeId, Message>,
    pub completions: BTreeMap<NodeId, LogicalNetworkCompletion>,
    pub endpoint: LogicalEndpointChain,
    pub reason: LogicalNodeError,
}
pub type LogicalNodeRecoveryParts<S, T, E, A, U, C, V, B> = LogicalNodeRecovery<
    LogicalNodeParts<S, T, E, A, U, C, V, B>,
    <A as StateMachine>::Receipt,
    <A as ReadableStateMachine>::ReadResult,
>;
struct NodeSession<U: SecureSession, C, V, B> {
    session: LogicalPeerSession<U, C, V, B>,
    incoming: Option<Message>,
    failed_completion: Option<LogicalNetworkCompletion>,
    shutdown_closure: Option<LogicalNetworkError>,
}
/// Caller-polled logical owner. Only consumed original child observations permit
/// the shared ReplicaDriver to release persistence, protocol and client effects.
pub struct LogicalNode<
    S: ReadyScheduler,
    T: TimerService,
    E: ElectionEntropy,
    A: StateMachine + ReadableStateMachine,
    U: SecureSession = Box<dyn SecureSession>,
    C = NativeLogCodec,
    V = NativeSnapshotCodec,
    B = NativeWireCodec,
> where
    A::Receipt: ApplicationReceipt,
{
    local: LogicalNodeLocal<S, T, E, A>,
    persistence: LogicalPersistencePort,
    snapshots: Option<LogicalSnapshotPort>,
    replica: LogicalReplicaDriver<A::Receipt, A::ReadResult>,
    configuration: ConfigurationRequests,
    endpoint: LogicalEndpointChain,
    group: GroupIdentity,
    sessions: BTreeMap<NodeId, NodeSession<U, C, V, B>>,
    pending: VecDeque<LogicalNetworkBatch>,
    deferred: Option<(NodeId, StoreIdentity)>,
    session_cursor: Option<NodeId>,
    state: NodeState,
    now: MonoTime,
    failure: Option<LogicalNodeError>,
}
