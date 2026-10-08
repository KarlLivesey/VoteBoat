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
//! Owning node facade over the same public local and peer providers.
use super::*;
use crate::{
    application::*, connect::PeerConnector, outbound::*, snapshot_worker::*, transport::*,
    worker::*,
};

pub struct NodeSnapshots<H: SnapshotWorker> {
    pub router: SnapshotRouter,
    pub worker: H,
}
pub struct NodeLocalParts<
    S: ReadyScheduler,
    T: TimerService,
    E: ElectionEntropy,
    A: StateMachine + ReadableStateMachine,
    W: PersistenceWorker,
    O: OutboundQueue,
    H: SnapshotWorker,
> where
    A::Receipt: ApplicationReceipt,
{
    pub owner: EffectOwner<S, T, E>,
    pub persistence: W,
    pub applications: BTreeMap<GroupIdentity, A>,
    pub results: ApplicationRouter<A::Receipt>,
    pub clients: ClientRouter<A::Receipt>,
    pub reads: ReadRequests<A::Query, A::ReadResult>,
    pub outbound: O,
    pub snapshots: Option<NodeSnapshots<H>>,
}
impl<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: StateMachine + ReadableStateMachine,
        W: PersistenceWorker,
        O: OutboundQueue,
        H: SnapshotWorker,
    > NodeLocalParts<S, T, E, A, W, O, H>
where
    A::Receipt: ApplicationReceipt,
{
    /// Reborrow only selected components; no replacement or fallback is created.
    pub fn as_parts(&mut self) -> ReplicaParts<'_, S, T, E, A, W, O> {
        ReplicaParts {
            owner: &mut self.owner,
            persistence: &mut self.persistence,
            applications: &mut self.applications,
            results: &mut self.results,
            clients: &mut self.clients,
            reads: &mut self.reads,
            outbound: &mut self.outbound,
            snapshots: self.snapshots.as_mut().map(|s| ReplicaSnapshots {
                router: &mut s.router,
                worker: &mut s.worker,
            }),
        }
    }
}
pub struct NodeParts<
    S: ReadyScheduler,
    T: TimerService,
    E: ElectionEntropy,
    A: StateMachine + ReadableStateMachine,
    W: PersistenceWorker,
    O: OutboundQueue,
    H: SnapshotWorker,
    C: PeerConnector,
    F: PeerTransportFactory<C::Session>,
> where
    A::Receipt: ApplicationReceipt,
{
    pub local: NodeLocalParts<S, T, E, A, W, O, H>,
    /// Omission is valid only when all configured groups have no remote voters.
    pub peers: Option<PeerParts<C, F>>,
}
#[derive(Clone, Copy, Debug, Default)]
pub struct NodeLimits {
    pub replica: ReplicaDriverLimits,
    pub peers: PeerDriverLimits,
}
#[derive(Clone, Copy, Debug, Default)]
pub struct NodePollBudget {
    pub replica: ReplicaPollBudget,
    pub peers: PeerDriverBudget,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeState {
    Running,
    Quiescing,
    Draining,
    Drained,
    RecoveryRequired,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodeError {
    InvalidLimits,
    TimeWentBack,
    Closed,
    RecoveryRequired,
    MissingPeers,
    WrongPeerStore,
    MissingSnapshots,
    Aborted,
    Owner(EffectOwnerError),
    Replica(ReplicaError),
    Peer(PeerDriverError),
    Client(ClientError),
    Read(ReadInvocationError),
}
pub struct NodeRejected<P> {
    pub reason: NodeError,
    pub parts: Box<P>,
}
pub type NodeBuild<N, P> = Result<N, NodeRejected<P>>;
pub type NodeReclaim<N, P> = Result<P, Box<N>>;
pub type NodeRecoveryParts<S, T, E, A, W, O, H, C, F> =
    NodeRecovery<NodeLocalParts<S, T, E, A, W, O, H>, <A as StateMachine>::Receipt, C, F>;
#[derive(Clone, Copy, Debug)]
pub enum NodeControl {
    Campaign,
    Heartbeat,
    Checkpoint,
}
#[derive(Debug)]
pub struct NodeProgress {
    pub state: NodeState,
    pub replica: Option<ReplicaProgress>,
    pub peers: Option<PeerDriverProgress>,
}
pub struct NodeRecovery<L, R, C: PeerConnector, F: PeerTransportFactory<C::Session>> {
    pub local: L,
    pub replica: ReplicaDriver<R>,
    pub peers: Option<PeerDriver<C, F>>,
    pub reason: NodeError,
}
/// One owner of selected component handles, not a process-wide service. Provider
/// scoped close/drain contracts leave shared host executors and unrelated users
/// intact. Construction and polling never create hidden infrastructure.
pub struct Node<
    S: ReadyScheduler,
    T: TimerService,
    E: ElectionEntropy,
    A: StateMachine + ReadableStateMachine,
    W: PersistenceWorker,
    O: OutboundQueue,
    H: SnapshotWorker,
    C: PeerConnector,
    F: PeerTransportFactory<C::Session>,
> where
    A::Receipt: ApplicationReceipt,
{
    local: NodeLocalParts<S, T, E, A, W, O, H>,
    replica: ReplicaDriver<A::Receipt>,
    peers: Option<PeerDriver<C, F>>,
    state: NodeState,
    now: MonoTime,
    failure: Option<NodeError>,
}
impl<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
        W: PersistenceWorker,
        O: OutboundQueue,
        H: SnapshotWorker,
        C: PeerConnector,
        F: PeerTransportFactory<C::Session>,
    > Node<S, T, E, A, W, O, H, C, F>
where
    A::Receipt: ApplicationReceipt,
    C::Endpoint: Clone,
{
    // Keep every selected provider visible and return its ownership on rejection.
    #[allow(clippy::type_complexity)]
    pub fn from_parts(
        mut parts: NodeParts<S, T, E, A, W, O, H, C, F>,
        limits: NodeLimits,
        now: MonoTime,
    ) -> NodeBuild<Self, NodeParts<S, T, E, A, W, O, H, C, F>> {
        let checked = (|| {
            parts
                .local
                .owner
                .validate_quiescent()
                .map_err(NodeError::Owner)?;
            if parts.peers.is_some() && parts.local.snapshots.is_none() {
                return Err(NodeError::MissingSnapshots);
            }
            for group in parts.local.owner.groups() {
                let core = parts.local.owner.core(group).unwrap();
                if core.state().base_index() > 0 && parts.local.snapshots.is_none() {
                    return Err(NodeError::MissingSnapshots);
                }
                for (peer, store) in core.membership().replicas() {
                    if peer == core.local_node() {
                        continue;
                    }
                    let network = parts.peers.as_ref().ok_or(NodeError::MissingPeers)?;
                    if network
                        .roster
                        .peer_identity(peer)
                        .is_none_or(|p| p.store != store)
                    {
                        return Err(NodeError::WrongPeerStore);
                    }
                }
            }
            ReplicaDriver::new(&parts.local.as_parts(), limits.replica, now)
                .map_err(NodeError::Replica)
        })();
        let replica = match checked {
            Ok(driver) => driver,
            Err(reason) => {
                return Err(NodeRejected {
                    reason,
                    parts: Box::new(parts),
                })
            }
        };
        let peers = if let Some(network) = parts.peers.take() {
            match PeerDriver::new(
                network,
                parts.local.owner.identity(),
                &parts.local.outbound,
                limits.peers,
                now,
            ) {
                Ok(driver) => Some(driver),
                Err(rejected) => {
                    parts.peers = Some(*rejected.parts);
                    return Err(NodeRejected {
                        reason: NodeError::Peer(rejected.reason),
                        parts: Box::new(parts),
                    });
                }
            }
        } else {
            None
        };
        Ok(Self {
            local: parts.local,
            replica,
            peers,
            state: NodeState::Running,
            now,
            failure: None,
        })
    }
    pub fn state(&self) -> NodeState {
        self.state
    }
    pub fn failure(&self) -> Option<&NodeError> {
        self.failure.as_ref()
    }
    pub fn local(&self) -> &NodeLocalParts<S, T, E, A, W, O, H> {
        &self.local
    }
    pub fn peers(&self) -> Option<&PeerDriver<C, F>> {
        self.peers.as_ref()
    }
    pub fn replica_usage(&self) -> ReplicaDriverUsage {
        self.replica.usage()
    }
    pub fn reclaim(&mut self, max_bytes: usize) -> Result<ReclaimTicket, NodeError> {
        if self.state != NodeState::Running {
            return Err(NodeError::Closed);
        }
        let result = self
            .replica
            .request_reclaim(&mut self.local.as_parts(), max_bytes)
            .map_err(NodeError::Replica);
        if let Err(reason) = &result {
            if self.replica.is_failed() {
                self.enter_recovery(reason.clone());
            }
        }
        result
    }
    pub fn poll_reclaim(&mut self) -> Option<ReclaimEvent> {
        self.replica.poll_reclaim()
    }
    pub fn propose(&mut self, request: ClientRequest) -> Result<ClientTicket, ClientRejected> {
        let Some(app) = self.local.applications.get(&request.group) else {
            return Err(ClientRejected {
                reason: ClientError::UnknownGroup,
                request,
            });
        };
        self.local
            .clients
            .submit(&mut self.local.owner, app, request)
    }
    pub fn read(
        &mut self,
        group: GroupIdentity,
        query: A::Query,
    ) -> Result<ReadInvocationTicket, ReadInvocationRejected<A::Query>> {
        let Some(app) = self.local.applications.get(&group) else {
            return Err(ReadInvocationRejected {
                reason: ReadInvocationError::UnknownGroup,
                group,
                query,
            });
        };
        self.local
            .reads
            .submit(&mut self.local.owner, app, group, query)
    }
    pub fn poll_client(&mut self) -> Option<ClientCompletion<A::Receipt>> {
        self.local.clients.poll()
    }
    pub fn complete_client(
        &mut self,
        output: ClientCompletion<A::Receipt>,
    ) -> Result<ClientOutcome<A::Receipt>, ClientCompletionRejected<A::Receipt>> {
        self.local.clients.complete(output)
    }
    pub fn poll_read(&mut self) -> Option<ReadCompletion<A::ReadResult>> {
        self.local.reads.poll()
    }
    pub fn complete_read(
        &mut self,
        output: ReadCompletion<A::ReadResult>,
    ) -> Result<ReadOutcome<A::ReadResult>, ReadCompletionRejected<A::ReadResult>> {
        self.local.reads.complete(output)
    }
    pub fn cancel_client(&mut self, ticket: ClientTicket) -> Result<(), ClientError> {
        self.local.clients.cancel_wait(ticket)
    }
    pub fn cancel_read(&mut self, ticket: ReadInvocationTicket) -> Result<(), ReadInvocationError> {
        self.local.reads.cancel_wait(ticket)
    }
    pub fn control(&mut self, group: GroupIdentity, control: NodeControl) -> Result<(), NodeError> {
        if self.state != NodeState::Running {
            return Err(NodeError::Closed);
        }
        let event = match control {
            NodeControl::Campaign => Event::Campaign,
            NodeControl::Heartbeat => Event::Heartbeat,
            NodeControl::Checkpoint => {
                if self.local.snapshots.is_none() {
                    return Err(NodeError::MissingSnapshots);
                }
                Event::Checkpoint
            }
        };
        self.local
            .owner
            .admit(group, event)
            .map_err(|r| NodeError::Owner(EffectOwnerError::Runtime(r.reason)))
    }
    pub fn disconnect(&mut self, peer: NodeId, now: MonoTime) -> Result<(), NodeError> {
        if self.state != NodeState::Running {
            return Err(NodeError::Closed);
        }
        if now < self.now {
            return Err(NodeError::TimeWentBack);
        }
        let network = self.peers.as_mut().ok_or(NodeError::MissingPeers)?;
        network.disconnect(peer, now).map_err(NodeError::Peer)?;
        self.now = now;
        Ok(())
    }
    /// Reconcile connection assignments for every hosted core against explicitly
    /// supplied routes and the connector's provisioned credentials. This changes
    /// networking only; it cannot activate membership or create a replica.
    pub fn reconcile_membership(
        &mut self,
        routes: BTreeMap<NodeId, crate::connect::ConnectDirection<C::Endpoint>>,
        now: MonoTime,
    ) -> Result<(), PeerReconcileRejected<C::Endpoint>> {
        if self.state != NodeState::Running || self.peers.is_none() {
            return Err(PeerReconcileRejected {
                reason: PeerDriverError::NotQuiescent,
                routes,
            });
        }
        if now < self.now {
            return Err(PeerReconcileRejected {
                reason: PeerDriverError::TimeWentBack,
                routes,
            });
        }
        self.peers
            .as_mut()
            .unwrap()
            .reconcile_membership(&self.local.owner, routes, now)?;
        self.now = now;
        Ok(())
    }
    pub fn begin_shutdown(&mut self) {
        if self.state == NodeState::Running {
            self.local.clients.close();
            self.local.reads.close();
            self.state = NodeState::Quiescing;
        }
    }
    pub fn poll(
        &mut self,
        now: MonoTime,
        budget: NodePollBudget,
    ) -> Result<NodeProgress, NodeError> {
        if !budget.replica.valid() {
            return Err(NodeError::InvalidLimits);
        }
        budget.peers.validate().map_err(NodeError::Peer)?;
        if now < self.now {
            return Err(NodeError::TimeWentBack);
        }
        if self.state == NodeState::RecoveryRequired {
            return Err(NodeError::RecoveryRequired);
        }
        self.now = now;
        if self.state == NodeState::Drained {
            return Ok(NodeProgress {
                state: self.state,
                replica: None,
                peers: None,
            });
        }
        let result = self.poll_inner(now, budget);
        if let Err(reason) = &result {
            self.enter_recovery(reason.clone());
        }
        result
    }
    fn poll_inner(
        &mut self,
        now: MonoTime,
        budget: NodePollBudget,
    ) -> Result<NodeProgress, NodeError> {
        let peers = self
            .peers
            .as_mut()
            .map(|p| {
                p.poll(
                    &mut self.local.owner,
                    &mut self.local.outbound,
                    now,
                    budget.peers,
                )
            })
            .transpose()
            .map_err(NodeError::Peer)?;
        let replica = self
            .replica
            .poll(&mut self.local.as_parts(), now, budget.replica)
            .map_err(NodeError::Replica)?;
        if self.state == NodeState::Quiescing
            && self.local.clients.is_drained()
            && self.local.reads.is_drained()
            && self.local.results.is_drained()
        {
            if let Some(p) = &mut self.peers {
                p.close();
            }
            self.local
                .owner
                .close_admission()
                .map_err(NodeError::Owner)?;
            self.state = NodeState::Draining;
        }
        if self.state == NodeState::Draining
            && self.local.owner.is_drained()
            && self.local.persistence.is_drained()
            && self.replica.is_drained()
            && self.local.outbound.is_drained()
            && self.peers.as_ref().is_none_or(|p| p.is_drained())
            && self
                .local
                .snapshots
                .as_ref()
                .is_none_or(|s| s.router.is_drained() && s.worker.is_drained())
        {
            self.local.results.close();
            self.local.persistence.close();
            if let Some(s) = &mut self.local.snapshots {
                s.worker.close();
            }
            self.local.outbound.close();
            self.state = NodeState::Drained;
        }
        Ok(NodeProgress {
            state: self.state,
            replica: Some(replica),
            peers,
        })
    }
    fn enter_recovery(&mut self, reason: NodeError) {
        // Both aborts preserve previously produced/consumer-held results, and
        // unknown pending writes are never reported as rolled back.
        let _ = self.local.clients.abort(&mut self.local.owner);
        let _ = self.local.reads.abort(&mut self.local.owner);
        if let Some(p) = &mut self.peers {
            p.close();
        }
        self.local.persistence.close();
        self.local.outbound.close();
        self.local.results.close();
        if let Some(s) = &mut self.local.snapshots {
            s.worker.close();
        }
        self.failure.get_or_insert(reason);
        self.state = NodeState::RecoveryRequired;
    }
    pub fn abort(&mut self) {
        if !matches!(self.state, NodeState::Drained | NodeState::RecoveryRequired) {
            self.enter_recovery(NodeError::Aborted);
        }
    }
    pub fn is_drained(&self) -> bool {
        self.state == NodeState::Drained
    }
    // Reclamation preserves the concrete selected provider types.
    #[allow(clippy::type_complexity)]
    pub fn into_parts(mut self) -> NodeReclaim<Self, NodeParts<S, T, E, A, W, O, H, C, F>> {
        if !self.is_drained() {
            return Err(Box::new(self));
        }
        let peers = if let Some(p) = self.peers.take() {
            match p.into_parts() {
                Ok(parts) => Some(parts),
                Err(driver) => {
                    self.peers = Some(*driver);
                    return Err(Box::new(self));
                }
            }
        } else {
            None
        };
        Ok(NodeParts {
            local: self.local,
            peers,
        })
    }
    // Recovery transfers all selected providers and outstanding driver ownership.
    #[allow(clippy::type_complexity)]
    pub fn into_recovery(self) -> NodeReclaim<Self, NodeRecoveryParts<S, T, E, A, W, O, H, C, F>> {
        if self.state != NodeState::RecoveryRequired {
            return Err(Box::new(self));
        }
        Ok(NodeRecovery {
            local: self.local,
            replica: self.replica,
            peers: self.peers,
            reason: self.failure.unwrap(),
        })
    }
}
