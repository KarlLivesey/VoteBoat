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
    pub configuration: ConfigurationRequestLimits,
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
    IncompatiblePeerProtocol,
    MissingSnapshots,
    Aborted,
    Owner(EffectOwnerError),
    Replica(ReplicaError),
    Peer(PeerDriverError),
    Client(ClientError),
    Read(ReadInvocationError),
    Configuration(ConfigurationRequestError),
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
    /// Queue a direct old-view witness query. Requires an authority-capable
    /// peer assembly; admission is neither a grant nor a durability receipt.
    AuthorizeReplication {
        witness: crate::secure::PeerIdentity,
        candidate: crate::secure::PeerIdentity,
        configuration: ConfigurationId,
    },
    /// Queue cancellation of the core's pending request and installed permit.
    CancelReplicationAuthorization,
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
    pub configuration: ConfigurationRequests,
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
    configuration: ConfigurationRequests,
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
            let configuration =
                ConfigurationRequests::new(parts.local.owner.identity(), limits.configuration)
                    .map_err(NodeError::Configuration)?;
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
                if core.configuration_replication_enabled()
                    && parts
                        .peers
                        .as_ref()
                        .is_none_or(|p| p.roster.wire_version() < 2)
                {
                    return Err(NodeError::IncompatiblePeerProtocol);
                }
                if core.learner_repair_wire_version().is_some_and(|version| {
                    parts
                        .peers
                        .as_ref()
                        .is_none_or(|p| p.roster.wire_version() != version)
                }) {
                    return Err(NodeError::IncompatiblePeerProtocol);
                }
                if core.state().base_index() > 0 && parts.local.snapshots.is_none() {
                    return Err(NodeError::MissingSnapshots);
                }
                let connection_peers = core
                    .connection_replicas(65536)
                    .map_err(|e| NodeError::Owner(EffectOwnerError::Consensus(e)))?;
                for (peer, store) in connection_peers {
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
            let budget = parts
                .peers
                .as_ref()
                .map(|network| {
                    let routes = match &network.admission_routes {
                        Some(routes) => routes.clone(),
                        None => network
                            .routes
                            .iter()
                            .map(|(&node, direction)| {
                                let peer = network
                                    .roster
                                    .peer_identity(node)
                                    .ok_or(NodeError::WrongPeerStore)?;
                                Ok((
                                    node,
                                    PeerRoute {
                                        store: peer.store,
                                        direction: direction.clone(),
                                    },
                                ))
                            })
                            .collect::<Result<BTreeMap<_, _>, NodeError>>()?,
                    };
                    let budget = PeerDriver::<C, F>::route_budget(network, &routes, limits.peers)
                        .map_err(NodeError::Peer)?;
                    let budget = parts
                        .local
                        .owner
                        .prepare_connection_budget(budget)
                        .map_err(NodeError::Owner)?;
                    Ok((budget, routes))
                })
                .transpose()?;
            let replica = ReplicaDriver::new(&parts.local.as_parts(), limits.replica, now)
                .map_err(NodeError::Replica)?;
            Ok((replica, budget, configuration))
        })();
        let (replica, budget, configuration) = match checked {
            Ok(driver) => driver,
            Err(reason) => {
                return Err(NodeRejected {
                    reason,
                    parts: Box::new(parts),
                })
            }
        };
        let mut peers = if let Some(network) = parts.peers.take() {
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
        if let Some((budget, routes)) = budget {
            parts.local.owner.install_connection_budget(budget);
            peers.as_mut().unwrap().install_admission_routes(routes);
        }
        Ok(Self {
            local: parts.local,
            replica,
            peers,
            state: NodeState::Running,
            now,
            failure: None,
            configuration,
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
            NodeControl::AuthorizeReplication {
                witness,
                candidate,
                configuration,
            } => {
                if self
                    .peers
                    .as_ref()
                    .is_none_or(|p| p.roster().wire_version() < 3)
                {
                    return Err(NodeError::IncompatiblePeerProtocol);
                }
                Event::AuthorizeReplication {
                    witness,
                    candidate,
                    configuration,
                }
            }
            NodeControl::CancelReplicationAuthorization => Event::CancelReplicationAuthorization,
        };
        self.local
            .owner
            .admit(group, event)
            .map_err(|r| NodeError::Owner(EffectOwnerError::Runtime(r.reason)))
    }
    /// Submit an administrative intent. Admission is volatile, not commitment.
    /// Drive it with poll_with_configuration_authorization; ordinary poll denies
    /// configuration execution. Host authorization must cover service scope,
    /// placement/failure domains and selected provider/wire capacities.
    pub fn configure(
        &mut self,
        request: ConfigurationRequest,
    ) -> Result<ConfigurationTicket, ConfigurationRejected> {
        if self.state != NodeState::Running {
            return Err(ConfigurationRejected {
                reason: ConfigurationRequestError::Closed,
                request: Box::new(request),
            });
        }
        self.configuration.submit(&mut self.local.owner, request)
    }
    pub fn poll_configuration(&mut self) -> Option<ConfigurationCompletion> {
        self.configuration.poll()
    }
    /// Historical local durable status, without a fresh cluster read barrier.
    /// Local absence does not establish non-execution or authorize a retry.
    pub fn configuration_status(
        &self,
        group: GroupIdentity,
        operation: OperationId,
    ) -> Result<crate::membership::ConfigurationOperationStatus, NodeError> {
        if self.state == NodeState::RecoveryRequired {
            return Err(NodeError::RecoveryRequired);
        }
        self.local
            .owner
            .core(group)
            .ok_or(NodeError::Owner(EffectOwnerError::Runtime(
                RuntimeError::UnknownGroup,
            )))?
            .configuration_status(operation)
            .map_err(|e| NodeError::Owner(EffectOwnerError::Consensus(e)))
    }
    /// Resume a recorded operation after lost observation/restart. Only a
    /// committed joint generates a final proposal, and execution still needs
    /// poll_with_configuration_authorization. No new learner intent is invented.
    pub fn resume_configuration(
        &mut self,
        group: GroupIdentity,
        operation: OperationId,
        requirements: ReadinessRequirements,
    ) -> Result<ConfigurationResumption, NodeError> {
        if self.state != NodeState::Running {
            return Err(NodeError::Closed);
        }
        use crate::membership::ConfigurationResumeAction;
        match self.configuration_status(group, operation)?.resume_action() {
            ConfigurationResumeAction::Completed => Ok(ConfigurationResumption::Completed),
            ConfigurationResumeAction::WaitForCommit => Ok(ConfigurationResumption::WaitForCommit),
            ConfigurationResumeAction::NotFoundLocally => {
                Ok(ConfigurationResumption::NotFoundLocally)
            }
            ConfigurationResumeAction::Finalize(record) => self
                .configure(ConfigurationRequest {
                    group,
                    proposal: ConfigurationProposal {
                        record,
                        readiness: vec![],
                        requirements,
                    },
                })
                .map(ConfigurationResumption::Submitted)
                .map_err(|e| NodeError::Configuration(e.reason)),
        }
    }
    /// Stops observation only; queued or persisted configuration work can commit.
    pub fn cancel_configuration(
        &mut self,
        ticket: ConfigurationTicket,
    ) -> Result<(), ConfigurationRequestError> {
        self.configuration.cancel_wait(ticket)
    }
    /// Admit a readiness round using the current authenticated native peer
    /// binding. Results are volatile on Raft::ready_learner; they require a
    /// fresh binding check again when a later promotion executes.
    pub fn request_learner_readiness(
        &mut self,
        group: GroupIdentity,
        learner: NodeId,
        requirements: crate::raft::ReadinessRequirements,
    ) -> Result<(), NodeError> {
        if self.state != NodeState::Running {
            return Err(NodeError::Closed);
        }
        let network = self.peers.as_ref().ok_or(NodeError::MissingPeers)?;
        let binding = network
            .roster()
            .binding(learner)
            .ok_or(NodeError::WrongPeerStore)?;
        if binding.wire_version < 4 {
            return Err(NodeError::IncompatiblePeerProtocol);
        }
        self.local
            .owner
            .admit(
                group,
                Event::CheckLearnerReadiness {
                    learner: crate::secure::PeerIdentity {
                        node: learner,
                        store: binding.peer.store.identity,
                    },
                    session: binding.peer.store.session,
                    requirements,
                },
            )
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
    /// Preview connection resources for one event across every hosted group.
    /// Borrows routes and leaves clocks, providers and core state unchanged.
    /// Success is not a reservation or protocol authority; recheck at execution.
    pub fn preflight_peer_event(
        &self,
        group: GroupIdentity,
        event: &crate::raft::Event,
        routes: &BTreeMap<NodeId, crate::connect::ConnectDirection<C::Endpoint>>,
        now: MonoTime,
    ) -> Result<(), PeerDriverError> {
        if self.state != NodeState::Running || self.peers.is_none() {
            return Err(PeerDriverError::NotQuiescent);
        }
        if now < self.now {
            return Err(PeerDriverError::TimeWentBack);
        }
        self.peers
            .as_ref()
            .unwrap()
            .preflight_event(&self.local.owner, group, event, routes, now)
    }
    /// Apply current-core connection assignments with owned route hints. This
    /// cannot activate membership or provision credentials.
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
            self.configuration.close(ConfigurationUnknown::Shutdown);
            self.state = NodeState::Quiescing;
        }
    }
    pub fn set_admission_routes(
        &mut self,
        routes: BTreeMap<NodeId, PeerRoute<C::Endpoint>>,
        now: MonoTime,
    ) -> Result<(), PeerRoutesRejected<C::Endpoint>> {
        if self.state != NodeState::Running || self.peers.is_none() {
            return Err(PeerRoutesRejected {
                reason: PeerDriverError::NotQuiescent,
                routes,
            });
        }
        if now < self.now {
            return Err(PeerRoutesRejected {
                reason: PeerDriverError::TimeWentBack,
                routes,
            });
        }
        self.peers
            .as_mut()
            .unwrap()
            .set_admission_routes(&mut self.local.owner, routes, now)?;
        self.now = now;
        Ok(())
    }
    pub fn poll(
        &mut self,
        now: MonoTime,
        budget: NodePollBudget,
    ) -> Result<NodeProgress, NodeError> {
        self.poll_with_configuration_authorization(now, budget, |_, _| {
            Err(ConfigurationProposalError::AuthenticationRequired)
        })
    }
    /// Apply the selected placement policy at execution, preserving all normal
    /// core, authenticated binding and selected transport envelope checks.
    /// Does not provision routes or enforce future application-state growth.
    pub fn poll_with_placement_authorizer(
        &mut self,
        now: MonoTime,
        budget: NodePollBudget,
        authorizer: &(impl crate::placement::PlacementAuthorizer + ?Sized),
    ) -> Result<NodeProgress, NodeError> {
        self.poll_with_configuration_authorization(now, budget, |core, proposal| {
            authorizer
                .authorize(
                    core.state().bootstrap.group,
                    core.membership(),
                    &proposal.record,
                )
                .map_err(ConfigurationProposalError::Placement)
        })
    }
    /// Rechecks host authorization and exact authenticated promotion bindings at
    /// execution, after peer polling and before any configuration persistence.
    /// The authorization callback must be bounded and nonblocking.
    /// Application commands still pass the existing application admission path.
    pub fn poll_with_configuration_authorization(
        &mut self,
        now: MonoTime,
        budget: NodePollBudget,
        mut authorize: impl FnMut(
            &Raft,
            &ConfigurationProposal,
        ) -> Result<(), ConfigurationProposalError>,
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
        let result = self.poll_inner(now, budget, &mut authorize);
        if let Err(reason) = &result {
            self.enter_recovery(reason.clone());
        }
        result
    }
    fn poll_inner(
        &mut self,
        now: MonoTime,
        budget: NodePollBudget,
        authorize: &mut impl FnMut(
            &Raft,
            &ConfigurationProposal,
        ) -> Result<(), ConfigurationProposalError>,
    ) -> Result<NodeProgress, NodeError> {
        if matches!(self.state, NodeState::Running | NodeState::Quiescing) {
            if let Some(peers) = &mut self.peers {
                peers
                    .reconcile_planned_membership(&self.local.owner, &self.local.outbound, now)
                    .map_err(NodeError::Peer)?;
            }
        }
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
        let network = self.peers.as_ref();
        let replica = self
            .replica
            .poll_authorized(
                &mut self.local.as_parts(),
                now,
                budget.replica,
                |core, event| {
                    let Event::Configure(proposal) = event else {
                        return Ok(());
                    };
                    authorize(core, proposal).map_err(RaftError::from)?;
                    if network.is_none() {
                        use crate::membership::ConfigurationChange;
                        if matches!(&proposal.record.change,
                            ConfigurationChange::Learners(next) | ConfigurationChange::Joint { next, .. }
                            if next.voter_stores().keys().chain(next.learners().keys()).any(|n| *n != core.local_node())) {
                            return Err(ConfigurationProposalError::MissingPeerTransport.into());
                        }
                    }
                    if network.is_some_and(|p| p.roster().wire_version() < 2) {
                        return Err(ConfigurationProposalError::UnsupportedWireVersion(
                            network.unwrap().roster().wire_version(),
                        )
                        .into());
                    }
                    for proof in &proposal.readiness {
                        let request = proof.ready.request();
                        let binding =
                            network.and_then(|p| p.roster().binding(request.learner.node));
                        if !binding.is_some_and(|b| {
                            b.wire_version >= 4 && b.peer.store == proof.authenticated
                        }) {
                            return Err(ConfigurationProposalError::AuthenticationRequired.into());
                        }
                    }
                    if let Some(network) = network {
                        network.configuration_capacity(core, proposal)
                            .map_err(ConfigurationProposalError::TransportCapacity)?;
                    }
                    Ok(())
                },
            )
            .map_err(NodeError::Replica)?;
        self.configuration
            .observe(&self.local.owner, &replica.steps)
            .map_err(NodeError::Configuration)?;
        if matches!(self.state, NodeState::Running | NodeState::Quiescing) {
            if let Some(peers) = &mut self.peers {
                peers
                    .reconcile_planned_membership(&self.local.owner, &self.local.outbound, now)
                    .map_err(NodeError::Peer)?;
            }
        }
        if self.state == NodeState::Quiescing
            && self.local.clients.is_drained()
            && self.local.reads.is_drained()
            && self.local.results.is_drained()
            && self.configuration.is_drained()
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
        self.configuration.close(ConfigurationUnknown::OwnerFailed);
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
            configuration: self.configuration,
        })
    }
}
