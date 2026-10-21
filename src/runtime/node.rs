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
use crate::{application::*, outbound::*, snapshot_worker::*, worker::*};
mod drain;
pub use drain::*;

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
pub struct NetworkNodeParts<
    S: ReadyScheduler,
    T: TimerService,
    E: ElectionEntropy,
    A: StateMachine + ReadableStateMachine,
    W: PersistenceWorker,
    O: OutboundQueue,
    H: SnapshotWorker,
    N: PeerNetwork,
> where
    A::Receipt: ApplicationReceipt,
{
    pub local: NodeLocalParts<S, T, E, A, W, O, H>,
    /// Omission is valid only when all configured groups have no remote voters.
    pub peers: Option<N::Parts>,
}
#[derive(Clone, Copy, Debug, Default)]
pub struct NodeLimits {
    pub replica: ReplicaDriverLimits,
    pub peers: PeerDriverLimits,
    pub configuration: ConfigurationRequestLimits,
    /// Automatic checkpoints and WAL reclaim by default; disabling them is a
    /// named choice that leaves every group bounded by its store limits.
    pub retention: RetentionMode,
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
    Draining,
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
    Maintenance(MaintenanceError),
}
pub struct NodeRejected<P> {
    pub reason: NodeError,
    pub parts: Box<P>,
}
pub type NodeBuild<N, P> = Result<N, NodeRejected<P>>;
pub type NodeReclaim<N, P> = Result<P, Box<N>>;
pub type NetworkNodeRecoveryParts<S, T, E, A, W, O, H, N> =
    NetworkNodeRecovery<NodeLocalParts<S, T, E, A, W, O, H>, <A as StateMachine>::Receipt, N>;
#[derive(Clone, Copy, Debug)]
pub enum NodeControl {
    /// Volatile handoff; the host owns durable intent, timeout and result.
    TransferLeadership(crate::raft::LeadershipTransferRequest),
    CancelLeadershipTransfer {
        context: crate::raft::RequestContext,
    },
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
impl NodeProgress {
    /// Whether this poll did replica, persistence, application or network work.
    /// Refusals, blocked ingress and peer status rows alone are not progress.
    pub fn made_progress(&self) -> bool {
        self.replica.as_ref().is_some_and(|r| {
            !r.steps.is_empty()
                || r.worker_events
                    + r.snapshot_events
                    + r.snapshot_installs
                    + r.snapshot_supplies
                    + r.effects
                    + r.retries
                    + r.persistence_batches
                    + r.sends
                    + r.applications
                    + r.reads
                    > 0
        }) || self.peers.as_ref().is_some_and(|p| {
            p.connection_submissions
                + p.connections
                + p.connection_failures
                + p.obsolete_connections
                + p.sends
                + p.received
                + p.ingress.admitted
                + p.ingress.discarded
                > 0
                || !p.completions.is_empty()
                || !p.ingress.completed.is_empty()
                || !p.ingress.rejected.is_empty()
        })
    }
}
pub struct NetworkNodeRecovery<L, R, N: PeerNetwork> {
    pub local: L,
    pub replica: ReplicaDriver<R>,
    pub peers: Option<N>,
    pub reason: NodeError,
    pub configuration: ConfigurationRequests,
    pub maintenance: WalMaintenanceStatus,
    pub checkpoints: CheckpointStatus,
}
/// One owner of selected component handles, not a process-wide service. Provider
/// scoped close/drain contracts leave shared host executors and unrelated users
/// intact. Construction and polling never create hidden infrastructure.
pub struct NetworkNode<
    S: ReadyScheduler,
    T: TimerService,
    E: ElectionEntropy,
    A: StateMachine + ReadableStateMachine,
    W: PersistenceWorker,
    O: OutboundQueue,
    H: SnapshotWorker,
    N: PeerNetwork,
> where
    A::Receipt: ApplicationReceipt,
{
    local: NodeLocalParts<S, T, E, A, W, O, H>,
    replica: ReplicaDriver<A::Receipt>,
    peers: Option<N>,
    state: NodeState,
    now: MonoTime,
    failure: Option<NodeError>,
    configuration: ConfigurationRequests,
    maintenance: WalMaintenanceStatus,
    // Whether the cores were last told the WAL is above high water.
    core_pressure: bool,
    // The data budget (`max_entries_per_group`) of the node's groups. They
    // share one store, so one set of log limits.
    entry_budget: usize,
    // The store's control reserve: WAL room above the high-water mark only
    // noop transitions may use (R4-1).
    wal_noop_reserve: usize,
    checkpoints: CheckpointStatus,
    retention: RetentionMode,
    drain: Option<drain::LocalDrain>,
    drain_history: Option<crate::drain::DrainRecord>,
}
impl<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
        W: PersistenceWorker,
        O: OutboundQueue,
        H: SnapshotWorker,
        N: PeerNetworkConfiguration,
    > NetworkNode<S, T, E, A, W, O, H, N>
where
    A::Receipt: ApplicationReceipt,
    N::Endpoint: Clone,
{
    // Keep every selected provider visible and return its ownership on rejection.
    #[allow(clippy::type_complexity)]
    pub fn from_parts(
        mut parts: NetworkNodeParts<S, T, E, A, W, O, H, N>,
        limits: NodeLimits,
        now: MonoTime,
    ) -> NodeBuild<Self, NetworkNodeParts<S, T, E, A, W, O, H, N>> {
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
            Self::validate_group_parts(&parts)?;
            let budget = parts
                .peers
                .as_ref()
                .map(|network| {
                    let routes = network.initial_routes().map_err(|reason| {
                        if reason == PeerDriverError::WrongBinding {
                            NodeError::WrongPeerStore
                        } else {
                            NodeError::Peer(reason)
                        }
                    })?;
                    let budget = network
                        .route_budget(&routes, limits.peers)
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
            let schedules = Self::default_schedules(&parts.local, limits.retention, now)?;
            Ok((replica, budget, configuration, schedules))
        })();
        let (replica, budget, configuration, (checkpoints, maintenance, entry_budget)) =
            match checked {
                Ok(driver) => driver,
                Err(reason) => {
                    return Err(NodeRejected {
                        reason,
                        parts: Box::new(parts),
                    })
                }
            };
        let mut peers = if let Some(network) = parts.peers.take() {
            match N::build(
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
        let wal_noop_reserve = Self::smallest_log_limits(&parts.local).control_reserve_bytes;
        let mut node = Self {
            local: parts.local,
            replica,
            peers,
            state: NodeState::Running,
            now,
            failure: None,
            configuration,
            maintenance,
            core_pressure: false,
            entry_budget,
            wal_noop_reserve,
            checkpoints,
            retention: limits.retention,
            drain: None,
            drain_history: None,
        };
        // R4-1: the shard knows its WAL budget before any step, so a campaign
        // requested before the first poll is already checked.
        let usage = node.local.persistence.wal_usage();
        node.publish_wal_budget(usage);
        Ok(node)
    }
    /// I5 / R4-1: the shard checks every live staging against this budget.
    fn publish_wal_budget(&mut self, usage: Option<crate::log::WalUsage>) {
        let budget = usage.filter(|u| u.capacity > 0).map(|u| super::WalBudget {
            limit: self.maintenance.high_water_bytes(u.capacity),
            published: u.bytes,
            reserve: self.wal_noop_reserve,
        });
        self.local.owner.set_wal_budget(budget);
    }
    /// Install the default maintenance a `Maintained` node runs from its first
    /// poll. Every refusal here is a construction error, never a silent stop.
    fn default_schedules(
        local: &NodeLocalParts<S, T, E, A, W, O, H>,
        retention: RetentionMode,
        now: MonoTime,
    ) -> Result<(CheckpointStatus, WalMaintenanceStatus, usize), NodeError> {
        let mut checkpoints = CheckpointStatus::default();
        let mut maintenance = WalMaintenanceStatus::default();
        let limits = Self::smallest_log_limits(local);
        // I5 / U5: whatever the retention mode, the WAL above the high-water
        // mark must hold the largest batch, or a batch could wait for ever.
        super::maintenance::check_wal_headroom(
            local.persistence.wal_usage(),
            maintenance.high_water_percent(),
            limits.max_batch_bytes,
            limits.control_reserve_bytes,
        )?;
        if retention == RetentionMode::LifetimeBounded {
            return Ok((checkpoints, maintenance, limits.max_entries_per_group));
        }
        if !Self::checkpoints_supported(local) {
            return Err(NodeError::Maintenance(
                MaintenanceError::CheckpointsUnavailable,
            ));
        }
        checkpoints.configure(
            Some(CheckpointPolicy::for_limits(limits)),
            now,
            limits.max_entries_per_group,
        )?;
        if let Some(ceiling) = local.persistence.reclaim_limit() {
            maintenance.configure(
                Some(WalMaintenancePolicy::for_ceiling(ceiling)),
                now,
                Some(ceiling),
            )?;
        }
        Ok((checkpoints, maintenance, limits.max_entries_per_group))
    }
    fn checkpoints_supported(local: &NodeLocalParts<S, T, E, A, W, O, H>) -> bool {
        local.snapshots.as_ref().is_some_and(|snapshots| {
            local.owner.groups().all(|group| {
                snapshots
                    .worker
                    .checkpoint_bytes(group)
                    .is_some_and(|n| n != 0)
            })
        })
    }
    /// The tightest retained-entry budget among the groups (store defaults
    /// when there are none yet).
    fn smallest_log_limits(local: &NodeLocalParts<S, T, E, A, W, O, H>) -> crate::log::LogLimits {
        local
            .owner
            .groups()
            .filter_map(|group| local.owner.core(group).map(|core| core.log_limits()))
            .min_by_key(|limits| limits.max_entries_per_group)
            .unwrap_or_default()
    }
    fn validate_group_parts(
        parts: &NetworkNodeParts<S, T, E, A, W, O, H, N>,
    ) -> Result<(), NodeError> {
        for group in parts.local.owner.groups() {
            let core = parts.local.owner.core(group).unwrap();
            if core.configuration_replication_enabled()
                && parts
                    .peers
                    .as_ref()
                    .is_none_or(|p| p.original_wire_version() < 2)
            {
                return Err(NodeError::IncompatiblePeerProtocol);
            }
            if core.learner_repair_wire_version().is_some_and(|version| {
                parts.peers.as_ref().is_none_or(|p| {
                    // Wire8 retains wire7 repair semantics and adds only
                    // the explicitly selected leadership handoff RPC.
                    !matches!(
                        (version, p.original_wire_version()),
                        (5, 5) | (6, 6) | (7, 7 | 8)
                    )
                })
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
                    .source_identity(peer)
                    .is_none_or(|p| p.store != store)
                {
                    return Err(NodeError::WrongPeerStore);
                }
            }
        }
        Ok(())
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
    pub fn peers(&self) -> Option<&N> {
        self.peers.as_ref()
    }
    /// Transfer an actual replacement channel through the selected network's
    /// checked lifecycle operation; never expose mutable network authority.
    pub fn replace_peer_channel(
        &mut self,
        channel: N::Channel,
        now: MonoTime,
    ) -> Result<N::Channel, (NodeError, Box<N::Channel>)>
    where
        N: PeerNetworkChannelReplacement,
    {
        if !matches!(self.state, NodeState::Running | NodeState::Quiescing) {
            return Err((NodeError::Closed, Box::new(channel)));
        }
        if now < self.now {
            return Err((NodeError::TimeWentBack, Box::new(channel)));
        }
        let Some(network) = &mut self.peers else {
            return Err((NodeError::MissingPeers, Box::new(channel)));
        };
        network
            .replace_channel(channel, now)
            .map_err(|r| (NodeError::Peer(r.reason), r.parts))
    }
    /// Stop admission to the selected adjacent channel, preserving the core
    /// and original collectors while normal polling settles actual local IO.
    pub fn retire_peer_channel(&mut self, now: MonoTime) -> Result<(), NodeError>
    where
        N: PeerNetworkChannelReplacement,
    {
        if !matches!(self.state, NodeState::Running | NodeState::Quiescing) {
            return Err(NodeError::Closed);
        }
        if now < self.now {
            return Err(NodeError::TimeWentBack);
        }
        self.peers
            .as_mut()
            .ok_or(NodeError::MissingPeers)?
            .retire_channel(now)
            .map_err(NodeError::Peer)
    }
    /// Non-owning diagnostics, including while draining. Does not poll the node
    /// or grant mutable access to its roster, providers or accepted work.
    pub fn peer_drain_status(&mut self, peer: NodeId) -> Option<N::DrainStatus>
    where
        N: PeerNetworkInspection,
    {
        self.peers.as_mut()?.peer_drain_status(peer)
    }
    /// Replace prepared peer credentials through the selected provider. The
    /// host must authorize and persist the rollout; this is not a Raft proposal.
    pub fn replace_peer_credentials(
        &mut self,
        expected: crate::authorization::CredentialGeneration,
        replacement: crate::authorization::CredentialGeneration,
        material: <N as PeerNetworkCredentials>::Credentials,
    ) -> Result<
        <N as PeerNetworkCredentials>::Credentials,
        (
            crate::connect::ConnectError,
            <N as PeerNetworkCredentials>::Credentials,
        ),
    >
    where
        N: PeerNetworkCredentials,
    {
        if self.state != NodeState::Running {
            return Err((crate::connect::ConnectError::Closed, material));
        }
        let Some(peers) = self.peers.as_mut() else {
            return Err((crate::connect::ConnectError::Closed, material));
        };
        peers.replace_peer_credentials(expected, replacement, material)
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
    /// Take only manually requested results. Automatic results are retained
    /// separately in wal_maintenance(), including fatal terminal results.
    pub fn poll_reclaim(&mut self) -> Option<ReclaimEvent> {
        if self.maintenance.pending.is_some() {
            None
        } else {
            self.replica.poll_reclaim()
        }
    }
    /// Install/disable local physical maintenance. Accepted jobs must finish
    /// first. Manual receipts are never consumed by this schedule. The first
    /// deadline is relative to this Node's last validated host time.
    pub fn configure_wal_maintenance(
        &mut self,
        policy: Option<WalMaintenancePolicy>,
    ) -> Result<(), NodeError> {
        if self.state != NodeState::Running {
            return Err(NodeError::Closed);
        }
        if policy.is_none() && self.retention == RetentionMode::Maintained {
            return Err(NodeError::Maintenance(
                MaintenanceError::DisableRequiresLifetimeBounded,
            ));
        }
        if let Some(p) = policy {
            let limits = Self::smallest_log_limits(&self.local);
            super::maintenance::check_wal_headroom(
                self.local.persistence.wal_usage(),
                p.high_water_percent,
                limits.max_batch_bytes,
                limits.control_reserve_bytes,
            )?;
        }
        self.maintenance
            .configure(policy, self.now, self.local.persistence.reclaim_limit())
    }
    pub fn wal_maintenance(&self) -> &WalMaintenanceStatus {
        &self.maintenance
    }
    /// Protocol time of the latest poll (local monotonic clock).
    pub fn poll_time(&self) -> MonoTime {
        self.now
    }
    /// The retention mode chosen at construction.
    pub fn retention(&self) -> RetentionMode {
        self.retention
    }
    /// Latest physical WAL occupancy the persistence provider published.
    pub fn wal_usage(&self) -> Option<crate::log::WalUsage> {
        self.local.persistence.wal_usage()
    }
    /// Configure fair, bounded local checkpoints. This never bypasses the
    /// selected snapshot worker, application validation or durable log update.
    pub fn configure_checkpoints(
        &mut self,
        policy: Option<CheckpointPolicy>,
    ) -> Result<(), NodeError> {
        if self.state != NodeState::Running {
            return Err(NodeError::Closed);
        }
        if policy.is_none() && self.retention == RetentionMode::Maintained {
            return Err(NodeError::Maintenance(
                MaintenanceError::DisableRequiresLifetimeBounded,
            ));
        }
        if policy.is_some() {
            let snapshots = self
                .local
                .snapshots
                .as_ref()
                .ok_or(NodeError::MissingSnapshots)?;
            for group in self.local.owner.groups() {
                if snapshots
                    .worker
                    .checkpoint_bytes(group)
                    .is_none_or(|n| n == 0)
                {
                    return Err(NodeError::MissingSnapshots);
                }
            }
        }
        let max_entries = Self::smallest_log_limits(&self.local).max_entries_per_group;
        self.checkpoints.configure(policy, self.now, max_entries)
    }
    pub fn checkpoints(&self) -> &CheckpointStatus {
        &self.checkpoints
    }
    /// Read-only export of the selected live worker's exact durable anchor.
    /// This data is not a learner-readiness or quorum receipt.
    pub fn export_checkpoint(
        &mut self,
        group: GroupIdentity,
    ) -> Result<SnapshotExportTicket, SnapshotRouteError> {
        if self.state != NodeState::Running {
            return Err(SnapshotRouteError::Closed);
        }
        let snapshots = self
            .local
            .snapshots
            .as_mut()
            .ok_or(SnapshotRouteError::NotSnapshot)?;
        let app = self
            .local
            .applications
            .get(&group)
            .ok_or(SnapshotRouteError::UnknownGroup)?;
        let result = snapshots.router.export_checkpoint(
            &mut self.local.owner,
            &mut snapshots.worker,
            app,
            group,
        );
        if self.local.owner.is_failed() {
            self.enter_recovery(NodeError::Owner(EffectOwnerError::ProviderContract));
        }
        result
    }
    pub fn poll_checkpoint_export(&mut self) -> Option<SnapshotExportCompletion> {
        self.local.snapshots.as_mut()?.router.poll_export()
    }
    pub fn complete_checkpoint_export(
        &mut self,
        completion: SnapshotExportCompletion,
    ) -> Result<SnapshotExportOutcome, SnapshotExportRejected> {
        let Some(snapshots) = self.local.snapshots.as_mut() else {
            return Err(SnapshotExportRejected {
                reason: SnapshotRouteError::NotSnapshot,
                completion: Box::new(completion),
            });
        };
        snapshots.router.complete_export(completion)
    }
    pub fn cancel_checkpoint_export(
        &mut self,
        ticket: SnapshotExportTicket,
    ) -> Result<(), SnapshotRouteError> {
        self.local
            .snapshots
            .as_mut()
            .ok_or(SnapshotRouteError::NotSnapshot)?
            .router
            .cancel_export(ticket)
    }
    /// Only after failure, replace the ordinary snapshot consumer with bounded
    /// terminal cleanup. Held export results still need original completion.
    pub fn drain_failed_checkpoint_exports(
        &mut self,
        limit: usize,
    ) -> Result<usize, SnapshotRouteError> {
        if self.state != NodeState::RecoveryRequired {
            return Err(SnapshotRouteError::ProviderContract);
        }
        let snapshots = self
            .local
            .snapshots
            .as_mut()
            .ok_or(SnapshotRouteError::NotSnapshot)?;
        snapshots
            .router
            .drain_failed_exports(&mut self.local.owner, &mut snapshots.worker, limit)
    }
    pub fn propose(&mut self, request: ClientRequest) -> Result<ClientTicket, ClientRejected> {
        if self.drain.is_some() {
            return Err(ClientRejected {
                reason: ClientError::Draining,
                request,
            });
        }
        self.propose_maintenance(request)
    }
    /// Host-authorized maintenance admission during a local drain. This grants
    /// no authorization itself; normal application and consensus checks apply.
    pub fn propose_maintenance(
        &mut self,
        request: ClientRequest,
    ) -> Result<ClientTicket, ClientRejected> {
        let Some(app) = self.local.applications.get(&request.group) else {
            return Err(ClientRejected {
                reason: ClientError::UnknownGroup,
                request,
            });
        };
        // ST2b: the high-water state refuses a client before the request
        // reaches any core or log. R6-3: admission reads the state the last
        // poll computed (each poll folds in the WAL's published occupancy and
        // the shard's refusals) instead of recomputing it per proposal. The
        // byte bound itself is checked for every proposal at its step
        // (`WalAccount::over`, the same typed `StoragePressure`), so one that
        // arrives before the next poll notices the WAL thread's latest
        // publication is refused there, never admitted past the mark.
        if self.maintenance.high_water {
            self.maintenance.refused_proposal();
            return Err(ClientRejected {
                reason: ClientError::StoragePressure,
                request,
            });
        }
        // ST2 round 3 (I2): the group's retained-entry budget gets the same
        // typed, retryable refusal, with the same hysteresis, while
        // checkpoints are on to bring it back down (an opt-out node refuses
        // at the budget itself, also typed).
        if let Some(core) = self
            .local
            .owner
            .core(request.group)
            .filter(|_| self.retention == RetentionMode::Maintained)
        {
            if self.maintenance.entry_high_water(
                request.group,
                core.state().entries.len(),
                core.log_limits().max_entries_per_group,
            ) {
                self.maintenance.refused_entry_proposal();
                return Err(ClientRejected {
                    reason: ClientError::StoragePressure,
                    request,
                });
            }
        }
        self.local
            .clients
            .submit(&mut self.local.owner, app, request)
    }
    pub fn read(
        &mut self,
        group: GroupIdentity,
        query: A::Query,
    ) -> Result<ReadInvocationTicket, ReadInvocationRejected<A::Query>> {
        if self.drain.is_some() {
            return Err(ReadInvocationRejected {
                reason: ReadInvocationError::Draining,
                group,
                query,
            });
        }
        self.read_maintenance(group, query)
    }
    /// Explicit host-authorized maintenance query. Reads still require the
    /// ordinary fresh quorum barrier; a drain never supplies read authority.
    pub fn read_maintenance(
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
            NodeControl::TransferLeadership(request) => {
                if self
                    .peers
                    .as_ref()
                    .is_none_or(|p| p.original_wire_version() < 8)
                {
                    return Err(NodeError::IncompatiblePeerProtocol);
                }
                Event::TransferLeadership(request)
            }
            NodeControl::CancelLeadershipTransfer { context } => {
                Event::CancelLeadershipTransfer { context }
            }
            NodeControl::Campaign if self.drain.is_some() => return Err(NodeError::Draining),
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
                    .is_none_or(|p| p.original_wire_version() < 3)
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
    /// Whether this owner's original administrative observer stream is empty.
    /// This does not report configuration commitment or whole-node quiescence.
    pub fn configuration_requests_are_drained(&self) -> bool {
        self.configuration.is_drained()
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
        self.request_learner_readiness_tracked(group, learner, requirements)
            .map(|_| ())
    }
    /// The original owner-scoped admission for matching its later OwnerStep.
    /// Neither admission nor successful execution is promotion readiness.
    pub fn request_learner_readiness_tracked(
        &mut self,
        group: GroupIdentity,
        learner: NodeId,
        requirements: crate::raft::ReadinessRequirements,
    ) -> Result<AdmissionTicket, NodeError> {
        if self.state != NodeState::Running {
            return Err(NodeError::Closed);
        }
        let network = self.peers.as_ref().ok_or(NodeError::MissingPeers)?;
        let source = network
            .source_view(learner)
            .ok_or(NodeError::WrongPeerStore)?;
        if source.source().node != learner {
            return Err(NodeError::WrongPeerStore);
        }
        if source.original_wire_version() < 4 {
            return Err(NodeError::IncompatiblePeerProtocol);
        }
        self.local
            .owner
            .admit_tracked(
                group,
                Event::CheckLearnerReadiness {
                    learner: crate::secure::PeerIdentity {
                        node: learner,
                        store: source.source().store.identity,
                    },
                    session: source.source().store.session,
                    requirements: Box::new(requirements),
                },
            )
            .map_err(|r| NodeError::Owner(EffectOwnerError::Runtime(r.reason)))
    }
    /// Queue cancellation of volatile readiness work/results. This revokes no
    /// durable membership and takes effect only when the owner executes it.
    /// A host may then start a fresh round after a lost reply or changed session.
    pub fn cancel_learner_readiness(&mut self, group: GroupIdentity) -> Result<(), NodeError> {
        self.cancel_learner_readiness_tracked(group).map(|_| ())
    }
    /// Original owner-scoped cancellation admission; observe its matching step
    /// before replacing a volatile readiness exchange.
    pub fn cancel_learner_readiness_tracked(
        &mut self,
        group: GroupIdentity,
    ) -> Result<AdmissionTicket, NodeError> {
        if self.state != NodeState::Running {
            return Err(NodeError::Closed);
        }
        self.local
            .owner
            .admit_tracked(group, Event::CancelLearnerReadiness)
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
        routes: &BTreeMap<NodeId, crate::connect::ConnectDirection<N::Endpoint>>,
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
        routes: BTreeMap<NodeId, crate::connect::ConnectDirection<N::Endpoint>>,
        now: MonoTime,
    ) -> Result<(), PeerReconcileRejected<N::Endpoint>> {
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
        if let Some(snapshots) = &mut self.local.snapshots {
            snapshots.router.close_exports();
        }
        if self.state == NodeState::Running {
            self.local.clients.close();
            self.local.reads.close();
            self.configuration.close(ConfigurationUnknown::Shutdown);
            self.state = NodeState::Quiescing;
        }
    }
    pub fn set_admission_routes(
        &mut self,
        routes: BTreeMap<NodeId, PeerRoute<N::Endpoint>>,
        now: MonoTime,
    ) -> Result<(), PeerRoutesRejected<N::Endpoint>> {
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
    /// Earliest deadline at which a poll has timer, retry or connection work
    /// even without input: a host that waits on `readiness_fds` must wake by
    /// then. Advisory: polling remains the only way any of it executes.
    pub fn next_deadline(&self) -> Option<MonoTime> {
        [
            self.local.owner.next_deadline(),
            self.replica.next_deadline(),
            self.peers.as_ref().and_then(PeerNetwork::next_deadline),
        ]
        .into_iter()
        .flatten()
        .min()
    }
    /// Descriptors whose readability means the next poll may have network work.
    /// Advisory: a host may wait on them (and its worker wake) instead of a
    /// fixed park; polling remains the only source of progress.
    #[cfg(unix)]
    pub fn readiness_fds(&self, out: &mut Vec<crate::worker::ReadinessFd>) {
        let first = out.len();
        if let Some(peers) = &self.peers {
            peers.readiness_fds(out);
        }
        if !self.local.outbound.is_drained() {
            // Queued output is flushed by the next poll once a socket can take it.
            for fd in &mut out[first..] {
                fd.write = true;
            }
        }
        if self.local.owner.has_ready_input() || self.replica.has_queued_persists() {
            out.push(crate::worker::ReadinessFd::immediate());
        }
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
        // Observe even a fatal terminal result before transferring recovery
        // ownership. The driver has already checked its exact request scope.
        if self.maintenance.pending.is_some() {
            if let Some(event) = self.replica.poll_reclaim() {
                self.maintenance.completed(event, now);
            }
        }
        if let Err(reason) = &result {
            self.enter_recovery(reason.clone());
        }
        let usage = self.local.persistence.wal_usage();
        let account = self.local.owner.wal_account();
        self.maintenance
            .note_budget_refusals(account.refused.saturating_add(account.declined));
        self.maintenance.note_noop_refusals(account.noop_refused);
        let high_water = self.maintenance.update_high_water(usage);
        if usage.is_some_and(|u| u.parked > 0) {
            self.maintenance.held_since.get_or_insert(now);
        } else {
            self.maintenance.held_since = None;
        }
        self.publish_wal_budget(usage);
        // I2: groups leave the entry gate without waiting for a proposal.
        if !self.maintenance.entry_high_water.is_empty() {
            for group in self.maintenance.entry_high_water.clone() {
                if let Some(core) = self.local.owner.core(group) {
                    let (retained, max) = (
                        core.state().entries.len(),
                        core.log_limits().max_entries_per_group,
                    );
                    self.maintenance.entry_high_water(group, retained, max);
                }
            }
        }
        // Cores report the high-water state (`Raft::storage_pressure`). Their
        // followers decide on the byte room the shard gives before every
        // received message (R6-1), not on this latch. Set on every poll
        // while it lasts (groups may be added), cleared once.
        if high_water || self.core_pressure {
            self.local.owner.set_storage_pressure(high_water);
            self.core_pressure = high_water;
        }
        // A held batch only a rewrite can unpark must not stall a drain.
        let held = usage.is_some_and(|u| u.parked > 0)
            && matches!(self.state, NodeState::Quiescing | NodeState::Draining);
        if result.is_ok() && (self.state == NodeState::Running || held) {
            let pressure = high_water || super::maintenance::pressure(usage);
            let entry_high = super::maintenance::percent_of(
                self.entry_budget,
                self.maintenance.high_water_percent(),
            ) as u64;
            self.checkpoints.schedule(
                &mut self.local.owner,
                &self.local.applications,
                now,
                pressure,
                entry_high,
            );
            if let Err(reason) = self.schedule_wal_maintenance(now, usage, pressure) {
                self.enter_recovery(reason.clone());
                return Err(reason);
            }
        }
        result
    }
    fn schedule_wal_maintenance(
        &mut self,
        now: MonoTime,
        usage: Option<crate::log::WalUsage>,
        pressure: bool,
    ) -> Result<(), NodeError> {
        // The live estimate is O(groups); compute it only when a rewrite may
        // be due (under pressure or at the interval deadline).
        let live = if pressure || self.maintenance.interval_due(now) {
            self.local
                .owner
                .groups()
                .filter_map(|g| self.local.owner.core(g))
                .fold(0usize, |sum, core| {
                    sum.saturating_add(core.live_log_bytes())
                })
        } else {
            0
        };
        let Some(policy) = self.maintenance.due(now, usage, live) else {
            return Ok(());
        };
        match self.reclaim(policy.max_bytes) {
            Ok(ticket) => self.maintenance.submitted(ticket, now),
            Err(NodeError::Replica(ReplicaError::Worker(error))) => {
                self.maintenance.rejected(error.clone(), now);
                if self.state == NodeState::RecoveryRequired {
                    return Err(NodeError::Replica(ReplicaError::Worker(error)));
                }
            }
            Err(error) => return Err(error),
        }
        Ok(())
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
        if self.state == NodeState::Running {
            self.poll_drain_gate()?;
        }
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
                |core, event| Self::authorize_configuration(network, core, event, authorize),
            )
            .map_err(NodeError::Replica)?;
        self.observe_drain_gate(&replica.steps)?;
        self.checkpoints.observe(&self.local.owner, &replica.steps);
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
            && self.checkpoints.pending.is_empty()
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
    fn authorize_configuration(
        network: Option<&N>,
        core: &Raft,
        event: &Event,
        authorize: &mut impl FnMut(
            &Raft,
            &ConfigurationProposal,
        ) -> Result<(), ConfigurationProposalError>,
    ) -> Result<(), RaftError> {
        let Event::Configure(proposal) = event else {
            return Ok(());
        };
        authorize(core, proposal).map_err(RaftError::from)?;
        if network.is_none() {
            use crate::membership::ConfigurationChange;
            if matches!(&proposal.record.change,
                            ConfigurationChange::Learners(next) | ConfigurationChange::Joint { next, .. }
                            if next.voter_stores().keys().chain(next.learners().keys()).any(|n| *n != core.local_node()))
            {
                return Err(ConfigurationProposalError::MissingPeerTransport.into());
            }
        }
        if network.is_some_and(|p| p.original_wire_version() < 2) {
            return Err(ConfigurationProposalError::UnsupportedWireVersion(
                network.unwrap().original_wire_version(),
            )
            .into());
        }
        for proof in &proposal.readiness {
            let request = proof.ready.request();
            let source = network.and_then(|p| p.source_view(request.learner.node));
            if !source.is_some_and(|view| {
                view.source().node == request.learner.node
                    && view.original_wire_version() >= 4
                    && view.source().store == proof.authenticated
            }) {
                return Err(ConfigurationProposalError::AuthenticationRequired.into());
            }
        }
        if let Some(network) = network {
            network
                .configuration_capacity(core, proposal)
                .map_err(ConfigurationProposalError::TransportCapacity)?;
        }
        Ok(())
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
    pub fn into_parts(mut self) -> NodeReclaim<Self, NetworkNodeParts<S, T, E, A, W, O, H, N>> {
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
        Ok(NetworkNodeParts {
            local: self.local,
            peers,
        })
    }
    // Recovery transfers all selected providers and outstanding driver ownership.
    #[allow(clippy::type_complexity)]
    pub fn into_recovery(
        self,
    ) -> NodeReclaim<Self, NetworkNodeRecoveryParts<S, T, E, A, W, O, H, N>> {
        if self.state != NodeState::RecoveryRequired {
            return Err(Box::new(self));
        }
        Ok(NetworkNodeRecovery {
            local: self.local,
            replica: self.replica,
            peers: self.peers,
            reason: self.failure.unwrap(),
            configuration: self.configuration,
            maintenance: self.maintenance,
            checkpoints: self.checkpoints,
        })
    }
}

/// Compatibility direct-node facade over the same construction-selected runtime.
pub type Node<S, T, E, A, W, O, H, C, F> = NetworkNode<S, T, E, A, W, O, H, PeerDriver<C, F>>;
pub type NodeParts<S, T, E, A, W, O, H, C, F> =
    NetworkNodeParts<S, T, E, A, W, O, H, PeerDriver<C, F>>;
pub type NodeRecovery<L, R, C, F> = NetworkNodeRecovery<L, R, PeerDriver<C, F>>;
pub type NodeRecoveryParts<S, T, E, A, W, O, H, C, F> =
    NetworkNodeRecoveryParts<S, T, E, A, W, O, H, PeerDriver<C, F>>;
