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
//! Bounded peer reactor over explicitly owned connector, roster and ingress.
use super::*;
use crate::{connect::*, outbound::*, secure::*, transport::*};
use std::collections::BTreeSet;

pub struct PeerParts<C: PeerConnector, F: PeerTransportFactory<C::Session>> {
    pub connector: C,
    pub factory: F,
    pub roster: PeerRoster<F::Transport>,
    pub ingress: IngressRouter,
    /// Fixed address hints/directions for exactly the authorized roster peers.
    /// These cannot authorize membership, stores, sessions or application service.
    pub routes: BTreeMap<NodeId, ConnectDirection<C::Endpoint>>,
    /// Optional future connection hints retained through drain/recovery handoff.
    pub admission_routes: Option<BTreeMap<NodeId, PeerRoute<C::Endpoint>>>,
}
/// Owned hint for one exact provisioned store. Endpoint payloads must obey the
/// host connector's bounded endpoint contract; hints never provision credentials.
#[derive(Clone)]
pub struct PeerRoute<E> {
    pub store: StoreIdentity,
    pub direction: ConnectDirection<E>,
}
pub struct PeerRoutesRejected<E> {
    pub reason: PeerDriverError,
    pub routes: BTreeMap<NodeId, PeerRoute<E>>,
}
#[derive(Clone, Copy, Debug)]
pub struct PeerDriverLimits {
    pub staged_batches: usize,
    pub metadata_bytes: usize,
    /// Retain unsent output after an observed disconnect/connect failure for
    /// this bounded interval. Expiry returns local Failed, never remote evidence.
    /// Initial connection staging and usable-session backpressure are unaffected.
    pub disconnected_send_retry_ms: u64,
}
impl Default for PeerDriverLimits {
    fn default() -> Self {
        Self {
            staged_batches: 4096,
            metadata_bytes: 1024 * 1024,
            disconnected_send_retry_ms: 500,
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub struct PeerDriverBudget {
    pub connection_visits: usize,
    pub connector: ConnectPollBudget,
    pub peer_visits: usize,
    pub transport: TransportPollBudget,
    pub sends: usize,
    pub ingress: usize,
}
impl Default for PeerDriverBudget {
    fn default() -> Self {
        Self {
            connection_visits: 16,
            connector: ConnectPollBudget::default(),
            peer_visits: 16,
            transport: TransportPollBudget::default(),
            sends: 32,
            ingress: 128,
        }
    }
}
impl PeerDriverBudget {
    pub(super) fn validate(self) -> Result<(), PeerDriverError> {
        if self.connection_visits > 4096
            || self.peer_visits > 4096
            || self.sends > 4096
            || self.ingress > 65536
        {
            return Err(PeerDriverError::InvalidLimits);
        }
        self.connector
            .validate()
            .map_err(PeerDriverError::Connect)?;
        self.transport
            .validate()
            .map_err(PeerDriverError::Transport)?;
        Ok(())
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PeerDriverError {
    Owner(EffectOwnerError),
    InvalidLimits,
    WrongBinding,
    NotQuiescent,
    TimeWentBack,
    Fenced,
    ProviderViolation,
    Connect(ConnectError),
    Roster(PeerRosterError),
    Transport(TransportError),
    Outbound(OutboundError),
    Ingress(IngressError),
    Assignments(PeerAssignmentsError),
}
pub struct PeerReconcileRejected<E> {
    pub reason: PeerDriverError,
    pub routes: BTreeMap<NodeId, ConnectDirection<E>>,
}
pub struct PeerDriverRejected<C: PeerConnector, F: PeerTransportFactory<C::Session>> {
    pub reason: PeerDriverError,
    pub parts: Box<PeerParts<C, F>>,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PeerDriverUsage {
    pub attempts: usize,
    pub staged_batches: usize,
    pub quarantined_batches: usize,
    pub failed_send: bool,
    pub failed_receive: bool,
}
#[derive(Debug, Default)]
pub struct PeerDriverProgress {
    pub connection_submissions: usize,
    pub connections: usize,
    pub connection_failures: usize,
    pub obsolete_connections: usize,
    pub sends: usize,
    pub completions: Vec<LocalSendCompletion>,
    pub received: usize,
    pub ingress_blocked: usize,
    pub ingress: IngressProgress,
    pub peers: Vec<PeerPoll>,
}
/// Explicit recovery handoff. Original queue payloads remain charged until the
/// host resolves them against their exact queue; no field establishes delivery.
/// A violating provider may already have returned an oversized allocation, which
/// is quarantined whole after fencing, never admitted as normally bounded work.
pub struct PeerDriverRecovery<C: PeerConnector, F: PeerTransportFactory<C::Session>> {
    pub parts: PeerParts<C, F>,
    pub attempts: Vec<ConnectTicket>,
    pub staged: VecDeque<OutboundBatch>,
    pub quarantined: Vec<OutboundBatch>,
    pub failed_send: Option<Box<TransportSend>>,
    pub failed_receive: Option<Box<ReceivedBatch>>,
}
/// Single serialized peer reactor. All networking components are selected and
/// transferred explicitly at construction; the core and outbound queue stay
/// borrowed. No listener, clock, executor, thread or fallback is created here.
pub struct PeerDriver<C: PeerConnector, F: PeerTransportFactory<C::Session>> {
    parts: PeerParts<C, F>,
    owner: RuntimeOwner,
    outbound: OutboundBinding,
    limits: PeerDriverLimits,
    connector_limits: ConnectLimits,
    peers: Vec<NodeId>,
    cursor: usize,
    attempts: Vec<ConnectTicket>,
    staged: VecDeque<OutboundBatch>,
    quarantine: Vec<OutboundBatch>,
    failed_send: Option<Box<TransportSend>>,
    failed_receive: Option<Box<ReceivedBatch>>,
    now: MonoTime,
    closed: bool,
    failed: Option<PeerDriverError>,
}
impl<C: PeerConnector, F: PeerTransportFactory<C::Session>> PeerDriver<C, F> {
    pub fn new<O: OutboundQueue>(
        parts: PeerParts<C, F>,
        owner: RuntimeOwner,
        outbound: &O,
        limits: PeerDriverLimits,
        now: MonoTime,
    ) -> Result<Self, PeerDriverRejected<C, F>> {
        let check = (|| {
            let local = parts.roster.local();
            if local.store != owner.store
                || local.node != outbound.binding().node
                || parts.connector.local() != local
                || parts.roster.outbound_binding() != outbound.binding()
                || parts.ingress.binding().owner != owner
                || parts.ingress.binding().local != local
            {
                return Err(PeerDriverError::WrongBinding);
            }
            parts
                .connector
                .limits()
                .validate()
                .map_err(PeerDriverError::Connect)?;
            outbound
                .limits()
                .validate()
                .map_err(PeerDriverError::Outbound)?;
            let peers = parts.roster.authorized_peers().collect::<Vec<_>>();
            if let Some(routes) = &parts.admission_routes {
                Self::route_budget(&parts, routes, limits)?;
            }
            if peers.len() != parts.routes.len()
                || peers.iter().any(|p| !parts.routes.contains_key(p))
            {
                return Err(PeerDriverError::WrongBinding);
            }
            if parts.roster.limits().connect_timeout_ms > parts.connector.limits().timeout_ms {
                return Err(PeerDriverError::InvalidLimits);
            }
            let metadata = peers
                .len()
                .checked_mul(size_of::<NodeId>())
                .and_then(|n| {
                    n.checked_add(parts.connector.limits().requests * size_of::<ConnectTicket>())
                })
                .and_then(|n| {
                    n.checked_add(
                        limits
                            .staged_batches
                            .checked_mul(size_of::<OutboundBatch>())?,
                    )
                });
            if limits.staged_batches < outbound.limits().node.max.batches
                || limits.disconnected_send_retry_ms > 60_000
                || limits.staged_batches > 65536
                || limits.metadata_bytes > 64 * 1024 * 1024
                || metadata.is_none_or(|n| n > limits.metadata_bytes)
            {
                return Err(PeerDriverError::InvalidLimits);
            }
            if parts.connector.usage().requests != 0
                || parts.connector.usage().anonymous != 0
                || parts.roster.usage().connections != 0
                || parts.roster.is_fenced()
                || !parts.ingress.is_drained()
                || !outbound.is_drained()
            {
                return Err(PeerDriverError::NotQuiescent);
            }
            Ok(peers)
        })();
        let peers = match check {
            Ok(peers) => peers,
            Err(reason) => {
                return Err(PeerDriverRejected {
                    reason,
                    parts: Box::new(parts),
                })
            }
        };
        let attempts = Vec::with_capacity(parts.connector.limits().requests);
        let connector_limits = parts.connector.limits();
        Ok(Self {
            parts,
            owner,
            outbound: outbound.binding(),
            limits,
            connector_limits,
            peers,
            cursor: 0,
            attempts,
            staged: VecDeque::with_capacity(limits.staged_batches),
            quarantine: Vec::new(),
            failed_send: None,
            failed_receive: None,
            now,
            closed: false,
            failed: None,
        })
    }
    pub fn usage(&self) -> PeerDriverUsage {
        PeerDriverUsage {
            attempts: self.attempts.len(),
            staged_batches: self.staged.len(),
            quarantined_batches: self.quarantine.len(),
            failed_send: self.failed_send.is_some(),
            failed_receive: self.failed_receive.is_some(),
        }
    }
    pub fn limits(&self) -> PeerDriverLimits {
        self.limits
    }
    pub fn roster(&self) -> &PeerRoster<F::Transport> {
        &self.parts.roster
    }
    pub(super) fn configuration_capacity(
        &self,
        core: &Raft,
        proposal: &ConfigurationProposal,
    ) -> Result<crate::wire::ConfigurationWireCapacity, TransportError> {
        let capacity = self.parts.factory.configuration_capacity(
            &crate::wire::ConfigurationWireRequirements {
                bootstrap: &core.state().bootstrap,
                current: core.membership(),
                record: &proposal.record,
                index: core
                    .state()
                    .last_index()
                    .checked_add(1)
                    .ok_or(TransportError::ProviderViolation)?,
                committed_index: core.state().commit_index,
                application: proposal.requirements,
            },
        )?;
        self.parts.roster.check_configuration_capacity(&capacity)?;
        Ok(capacity)
    }
    pub fn ingress(&self) -> &IngressRouter {
        &self.parts.ingress
    }
    pub fn connector_usage(&self) -> ConnectUsage {
        self.parts.connector.usage()
    }
    /// Current credential generation, separate from connection generations.
    pub fn credential_generation(&self) -> Option<crate::authorization::CredentialGeneration>
    where
        C: PeerCredentialControl,
    {
        self.parts.connector.credential_generation()
    }
    /// Publish host-authorized prepared credentials without changing the roster.
    pub fn replace_peer_credentials(
        &mut self,
        expected: crate::authorization::CredentialGeneration,
        replacement: crate::authorization::CredentialGeneration,
        material: <C as PeerCredentialControl>::Credentials,
    ) -> Result<
        <C as PeerCredentialControl>::Credentials,
        (ConnectError, <C as PeerCredentialControl>::Credentials),
    >
    where
        C: PeerCredentialControl,
    {
        if self.closed || self.failed.is_some() {
            return Err((ConnectError::Closed, material));
        }
        self.parts
            .connector
            .replace_peer_credentials(expected, replacement, material)
    }
    pub fn is_failed(&self) -> bool {
        self.failed.is_some()
    }
    pub fn next_deadline(&self) -> Option<MonoTime> {
        if self.failed.is_some() {
            return None;
        }
        [
            self.parts.connector.next_deadline(),
            self.parts
                .roster
                .next_deadline_filtered(|peer| !self.attempts.iter().any(|t| t.peer.node == peer)),
            self.staged
                .iter()
                .filter_map(|batch| {
                    self.parts.roster.disconnected_send_deadline(
                        batch.ticket.peer,
                        self.limits.disconnected_send_retry_ms,
                    )
                })
                .min()
                .map(|deadline| deadline.max(self.now)),
        ]
        .into_iter()
        .flatten()
        .min()
    }
    /// Retained owned hints for provisioned exact stores, including future peers.
    pub fn admission_routes(&self) -> Option<&BTreeMap<NodeId, PeerRoute<C::Endpoint>>> {
        self.parts.admission_routes.as_ref()
    }
    pub(super) fn install_admission_routes(
        &mut self,
        routes: BTreeMap<NodeId, PeerRoute<C::Endpoint>>,
    ) {
        self.parts.admission_routes = Some(routes);
    }
    pub(super) fn route_budget(
        parts: &PeerParts<C, F>,
        routes: &BTreeMap<NodeId, PeerRoute<C::Endpoint>>,
        limits: PeerDriverLimits,
    ) -> Result<ConnectionBudget, PeerDriverError> {
        parts
            .connector
            .limits()
            .validate()
            .map_err(PeerDriverError::Connect)?;
        let bytes = routes
            .len()
            // Retained hints plus the owner's exact identity map; endpoint heap
            // payloads remain bounded by the host endpoint contract.
            .checked_mul(384usize.saturating_add(size_of::<PeerRoute<C::Endpoint>>()))
            .and_then(|n| n.checked_add(parts.roster.limits().peers * size_of::<NodeId>()))
            .and_then(|n| {
                n.checked_add(parts.connector.limits().requests * size_of::<ConnectTicket>())
            })
            .and_then(|n| {
                n.checked_add(
                    limits
                        .staged_batches
                        .checked_mul(size_of::<OutboundBatch>())?,
                )
            });
        if routes.len() > 65536 || bytes.is_none_or(|b| b > limits.metadata_bytes) {
            return Err(PeerDriverError::InvalidLimits);
        }
        if routes.iter().any(|(&node, route)| {
            node == parts.roster.local().node
                || !parts.connector.supports_peer(PeerIdentity {
                    node,
                    store: route.store,
                })
        }) {
            return Err(PeerDriverError::WrongBinding);
        }
        if parts.roster.authorized_peers().any(|node| {
            routes
                .get(&node)
                .is_none_or(|r| Some(r.store) != parts.roster.peer_identity(node).map(|p| p.store))
        }) {
            return Err(PeerDriverError::WrongBinding);
        }
        ConnectionBudget::new(
            parts.roster.local(),
            parts.roster.limits().peers,
            parts
                .roster
                .tracked_identities()
                .map(|p| (p.node, p.store))
                .collect(),
        )
        .and_then(|budget| {
            budget.with_provisioned(routes.iter().map(|(&n, r)| (n, r.store)).collect())
        })
        .map_err(|e| PeerDriverError::Owner(EffectOwnerError::Runtime(e)))
    }
    /// Install owned, pin-checked hints and an exact closed peer set on the owner.
    /// Replacement must cover all current/queued requirements. No connections or
    /// core transitions occur; failures return the complete proposed plan.
    pub fn set_admission_routes<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &mut self,
        owner: &mut EffectOwner<Q, T, E>,
        routes: BTreeMap<NodeId, PeerRoute<C::Endpoint>>,
        now: MonoTime,
    ) -> Result<(), PeerRoutesRejected<C::Endpoint>> {
        let prepared = (|| {
            self.check_assignment_owner(owner, now)?;
            let budget = Self::route_budget(&self.parts, &routes, self.limits)?;
            owner
                .prepare_connection_budget(budget)
                .map_err(PeerDriverError::Owner)
        })();
        let budget = match prepared {
            Ok(b) => b,
            Err(reason) => return Err(PeerRoutesRejected { reason, routes }),
        };
        owner.install_connection_budget(budget);
        self.parts.admission_routes = Some(routes);
        self.now = now;
        Ok(())
    }
    /// Use retained hints when core state needs a different connection set.
    /// Queue-only prospective peers are reserved but do not authorize connects.
    pub fn reconcile_planned_membership<
        Q: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        O: OutboundQueue,
    >(
        &mut self,
        owner: &EffectOwner<Q, T, E>,
        outbound: &O,
        now: MonoTime,
    ) -> Result<(), PeerDriverError>
    where
        C::Endpoint: Clone,
    {
        self.check_assignment_owner(owner, now)?;
        if outbound.binding() != self.outbound {
            return Err(PeerDriverError::WrongBinding);
        }
        let Some(plan) = &self.parts.admission_routes else {
            return Ok(());
        };
        let mut assignments = PeerAssignments::from_cores(
            self.parts.roster.local(),
            owner.groups().map(|g| owner.core(g).unwrap()),
            self.parts.roster.limits().peers,
        )
        .map_err(PeerDriverError::Assignments)?;
        for peer in self.parts.roster.authorized_peers() {
            if owner.has_pending_send(peer) || outbound.peer_usage(peer).batches > 0 {
                assignments
                    .retain_peer(
                        peer,
                        self.parts.roster.peer_identity(peer).unwrap().store,
                        self.parts.roster.limits().peers,
                    )
                    .map_err(PeerDriverError::Assignments)?;
            }
        }
        let desired = assignments.peers().collect::<BTreeMap<_, _>>();
        let routes = desired
            .iter()
            .map(|(&node, &store)| {
                let route = plan
                    .get(&node)
                    .filter(|r| r.store == store)
                    .ok_or(PeerDriverError::WrongBinding)?;
                Ok((node, route.direction.clone()))
            })
            .collect::<Result<BTreeMap<_, _>, PeerDriverError>>()?;
        if self.parts.roster.authorized_peers().count() == desired.len()
            && desired.iter().all(|(&n, &s)| {
                self.parts
                    .roster
                    .peer_identity(n)
                    .is_some_and(|p| p.store == s)
            })
        {
            // Adopt changed hints without canceling accepted work or resetting fairness.
            self.parts.routes = routes;
            return Ok(());
        }
        let peers = self.check_assignment_routes(&assignments, &routes, now)?;
        self.apply_assignments(assignments, peers, routes, now)
            .map_err(|r| r.reason)
    }
    /// Pure prospective inspection; success retains no capacity or authority.
    pub fn preflight_event<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &self,
        owner: &EffectOwner<Q, T, E>,
        group: GroupIdentity,
        event: &Event,
        routes: &BTreeMap<NodeId, ConnectDirection<C::Endpoint>>,
        now: MonoTime,
    ) -> Result<(), PeerDriverError> {
        self.check_assignment_owner(owner, now)?;
        let assignments = PeerAssignments::for_event(
            self.parts.roster.local(),
            owner.groups().map(|group| owner.core(group).unwrap()),
            group,
            event,
            self.parts.roster.limits().peers,
        )
        .map_err(PeerDriverError::Assignments)?;
        self.check_assignment_routes(&assignments, routes, now)?;
        Ok(())
    }
    fn check_assignment_owner<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &self,
        owner: &EffectOwner<Q, T, E>,
        now: MonoTime,
    ) -> Result<(), PeerDriverError> {
        if self.failed.is_some() || owner.is_failed() {
            return Err(PeerDriverError::Fenced);
        }
        if self.closed {
            return Err(PeerDriverError::NotQuiescent);
        }
        if now < self.now {
            return Err(PeerDriverError::TimeWentBack);
        }
        if owner.identity() != self.owner {
            return Err(PeerDriverError::WrongBinding);
        }
        Ok(())
    }
    fn check_assignment_routes(
        &self,
        assignments: &PeerAssignments,
        routes: &BTreeMap<NodeId, ConnectDirection<C::Endpoint>>,
        now: MonoTime,
    ) -> Result<Vec<NodeId>, PeerDriverError> {
        self.parts
            .roster
            .validate_assignments(assignments, now)
            .map_err(PeerDriverError::Roster)?;
        if routes.len() != assignments.peers().count()
            || assignments.peers().any(|(node, store)| {
                !routes.contains_key(&node)
                    || !self
                        .parts
                        .connector
                        .supports_peer(PeerIdentity { node, store })
            })
        {
            return Err(PeerDriverError::WrongBinding);
        }
        let peers = self
            .parts
            .roster
            .tracked_peers()
            .chain(assignments.peers().map(|(node, _)| node))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let metadata = peers
            .len()
            .checked_mul(size_of::<NodeId>())
            .and_then(|n| {
                n.checked_add(self.connector_limits.requests * size_of::<ConnectTicket>())
            })
            .and_then(|n| {
                n.checked_add(
                    self.limits
                        .staged_batches
                        .checked_mul(size_of::<OutboundBatch>())?,
                )
            });
        if metadata.is_none_or(|n| n > self.limits.metadata_bytes) {
            return Err(PeerDriverError::InvalidLimits);
        }
        Ok(peers)
    }
    /// Recompute all hosted groups' required connection stores at this serialized
    /// owner. Routes, credentials and resource ceilings are checked before mutation.
    /// Rejection returns owned routes; canceled attempts and accepted sends retain
    /// their original resources until terminal provider receipts.
    pub fn reconcile_membership<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &mut self,
        owner: &EffectOwner<Q, T, E>,
        routes: BTreeMap<NodeId, ConnectDirection<C::Endpoint>>,
        now: MonoTime,
    ) -> Result<(), PeerReconcileRejected<C::Endpoint>> {
        let prepared = (|| {
            self.check_assignment_owner(owner, now)?;
            let cores = owner
                .groups()
                .map(|group| owner.core(group))
                .collect::<Option<Vec<_>>>()
                .ok_or(PeerDriverError::WrongBinding)?;
            let assignments = PeerAssignments::from_cores(
                self.parts.roster.local(),
                cores,
                self.parts.roster.limits().peers,
            )
            .map_err(PeerDriverError::Assignments)?;
            let peers = self.check_assignment_routes(&assignments, &routes, now)?;
            Ok((assignments, peers))
        })();
        let (assignments, peers) = match prepared {
            Ok(prepared) => prepared,
            Err(reason) => return Err(PeerReconcileRejected { reason, routes }),
        };
        self.apply_assignments(assignments, peers, routes, now)
    }
    fn apply_assignments(
        &mut self,
        assignments: PeerAssignments,
        peers: Vec<NodeId>,
        routes: BTreeMap<NodeId, ConnectDirection<C::Endpoint>>,
        now: MonoTime,
    ) -> Result<(), PeerReconcileRejected<C::Endpoint>> {
        let canceled = match self.parts.roster.reconcile(&assignments, now) {
            Ok(canceled) => canceled,
            Err(reason) => {
                return Err(PeerReconcileRejected {
                    reason: PeerDriverError::Roster(reason),
                    routes,
                })
            }
        };
        self.parts.routes = routes;
        self.peers = peers;
        self.cursor = 0;
        self.now = now;
        for ticket in canceled {
            self.parts.connector.cancel(ticket);
        }
        Ok(())
    }
    pub fn is_drained(&self) -> bool {
        self.closed
            && self.attempts.is_empty()
            && self.staged.is_empty()
            && self.quarantine.is_empty()
            && self.failed_send.is_none()
            && self.failed_receive.is_none()
            && self.parts.connector.is_drained()
            && self.parts.roster.is_drained()
            && self.parts.ingress.is_drained()
    }
    /// Cancel the exact attempt before retiring this peer. Provider slots remain
    /// charged until terminal polling, and block replacement admission meanwhile.
    pub fn disconnect(&mut self, peer: NodeId, now: MonoTime) -> Result<(), PeerDriverError> {
        if now < self.now {
            return Err(PeerDriverError::TimeWentBack);
        }
        if self.failed.is_some() {
            return Err(PeerDriverError::Fenced);
        }
        self.now = now;
        if let Some(ticket) = self.attempts.iter().find(|t| t.peer.node == peer) {
            self.parts.connector.cancel(*ticket);
        }
        self.parts
            .roster
            .disconnect(peer, now)
            .map_err(PeerDriverError::Roster)
    }
    pub fn poll<O: OutboundQueue, Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &mut self,
        owner: &mut EffectOwner<Q, T, E>,
        outbound: &mut O,
        now: MonoTime,
        budget: PeerDriverBudget,
    ) -> Result<PeerDriverProgress, PeerDriverError>
    where
        C::Endpoint: Clone,
    {
        if owner.identity() != self.owner || outbound.binding() != self.outbound {
            return Err(PeerDriverError::WrongBinding);
        }
        budget.validate()?;
        if now < self.now {
            return Err(PeerDriverError::TimeWentBack);
        }
        if self.failed.is_some() || owner.is_failed() {
            return Err(PeerDriverError::Fenced);
        }
        self.now = now;
        let result = self.poll_inner(outbound, now, budget).and_then(|mut out| {
            out.ingress = self
                .parts
                .ingress
                .dispatch(&self.parts.roster, owner, budget.ingress)
                .map_err(PeerDriverError::Ingress)?;
            Ok(out)
        });
        if let Err(reason) = &result {
            let _ = owner.fail::<()>(EffectOwnerError::ProviderContract);
            self.fail_network(reason);
        }
        result
    }
    /// After close, resolve network ownership without accessing a failed core.
    /// Held ingress is canceled explicitly, never admitted or executed.
    pub fn drain<O: OutboundQueue>(
        &mut self,
        outbound: &mut O,
        now: MonoTime,
        budget: PeerDriverBudget,
    ) -> Result<PeerDriverProgress, PeerDriverError>
    where
        C::Endpoint: Clone,
    {
        if outbound.binding() != self.outbound {
            return Err(PeerDriverError::WrongBinding);
        }
        budget.validate()?;
        if now < self.now {
            return Err(PeerDriverError::TimeWentBack);
        }
        if self.failed.is_some() {
            return Err(PeerDriverError::Fenced);
        }
        if !self.closed {
            return Err(PeerDriverError::NotQuiescent);
        }
        self.now = now;
        let result = self.poll_inner(outbound, now, budget).map(|mut out| {
            out.ingress.completed = self.parts.ingress.abort();
            out.ingress.discarded = out.ingress.completed.iter().map(|c| c.discarded).sum();
            out
        });
        if let Err(reason) = &result {
            self.fail_network(reason);
        }
        result
    }
    fn fail_network(&mut self, reason: &PeerDriverError) {
        self.failed = Some(reason.clone());
        self.closed = true;
        self.parts.connector.close();
        self.parts.roster.abort();
        self.parts.ingress.close();
    }
    fn poll_inner<O: OutboundQueue>(
        &mut self,
        outbound: &mut O,
        now: MonoTime,
        b: PeerDriverBudget,
    ) -> Result<PeerDriverProgress, PeerDriverError>
    where
        C::Endpoint: Clone,
    {
        self.check_connector()?;
        if outbound.limits().validate().is_err()
            || outbound.limits().node.max.batches > self.limits.staged_batches
            || outbound.usage().batches > self.limits.staged_batches
        {
            return Err(PeerDriverError::ProviderViolation);
        }
        let peers = self
            .parts
            .roster
            .poll(now, b.peer_visits, b.transport)
            .map_err(PeerDriverError::Roster)?;
        let mut out = PeerDriverProgress {
            peers,
            ..Default::default()
        };
        for event in &out.peers {
            if let PeerPoll::ConnectExpired(ticket) = event {
                self.parts.connector.cancel(*ticket);
            }
            if matches!(
                event,
                PeerPoll::Disconnected {
                    error: Some(TransportError::ProviderViolation),
                    ..
                }
            ) {
                return Err(PeerDriverError::ProviderViolation);
            }
        }
        self.poll_connections(outbound, now, b.connector, &mut out)?;
        self.start_connections(now, b.connection_visits, &mut out)?;
        self.poll_peer_io(outbound, b.peer_visits, &mut out)?;
        self.submit_sends(outbound, b.sends, &mut out)?;
        Ok(out)
    }
    fn poll_connections<O: OutboundQueue>(
        &mut self,
        outbound: &mut O,
        now: MonoTime,
        budget: crate::connect::ConnectPollBudget,
        out: &mut PeerDriverProgress,
    ) -> Result<(), PeerDriverError> {
        let events = self
            .parts
            .connector
            .poll(now, budget)
            .map_err(PeerDriverError::Connect)?;
        if events.len() > budget.completions {
            return Err(PeerDriverError::ProviderViolation);
        }
        self.check_connector()?;
        for event in events {
            let index = self
                .attempts
                .iter()
                .position(|t| *t == event.ticket)
                .ok_or(PeerDriverError::ProviderViolation)?;
            self.attempts.swap_remove(index);
            if self.closed
                || self
                    .parts
                    .roster
                    .attempt_deadline(event.ticket)
                    .is_none_or(|d| now >= d)
            {
                // Dropping a late returned session closes its independently owned channel.
                out.obsolete_connections += 1;
                continue;
            }
            match event.result {
                Err(_) => {
                    self.parts
                        .roster
                        .connect_failed(event.ticket, now)
                        .map_err(PeerDriverError::Roster)?;
                    out.connection_failures += 1;
                }
                Ok(session) => {
                    let binding = require_authenticated(&session)
                        .map_err(|_| PeerDriverError::ProviderViolation)?;
                    if binding.local != event.ticket.local
                        || binding.peer.node != event.ticket.peer.node
                        || binding.peer.store.identity != event.ticket.peer.store
                        || binding.generation != event.ticket.generation
                        || binding.wire_version != self.parts.roster.wire_version()
                    {
                        return Err(PeerDriverError::ProviderViolation);
                    }
                    match self.parts.factory.build(session, outbound) {
                        Ok(transport) => {
                            if let Err(mut rejected) =
                                self.parts.roster.attach(event.ticket, transport, now)
                            {
                                rejected.transport.abort();
                                return Err(PeerDriverError::Roster(rejected.reason));
                            }
                            out.connections += 1;
                        }
                        Err(reason) => return Err(PeerDriverError::Transport(reason)),
                    }
                }
            }
        }
        Ok(())
    }
    fn start_connections(
        &mut self,
        now: MonoTime,
        visits: usize,
        out: &mut PeerDriverProgress,
    ) -> Result<(), PeerDriverError>
    where
        C::Endpoint: Clone,
    {
        if !self.closed {
            let active = &self.attempts;
            let mut slots = self
                .parts
                .connector
                .limits()
                .requests
                .saturating_sub(active.len());
            let tickets = self
                .parts
                .roster
                .due_connections_filtered(now, visits, |peer| {
                    if slots == 0 || active.iter().any(|t| t.peer.node == peer) {
                        false
                    } else {
                        slots -= 1;
                        true
                    }
                })
                .map_err(PeerDriverError::Roster)?;
            for ticket in tickets {
                let request = ConnectRequest {
                    ticket,
                    direction: self.parts.routes[&ticket.peer.node].clone(),
                    deadline: self.parts.roster.attempt_deadline(ticket).unwrap(),
                };
                match self.parts.connector.submit(request, now) {
                    Ok(()) => {
                        self.attempts.push(ticket);
                        out.connection_submissions += 1;
                    }
                    Err(rejected) => {
                        if rejected.request.ticket != ticket {
                            return Err(PeerDriverError::ProviderViolation);
                        }
                        self.parts
                            .roster
                            .connect_failed(ticket, now)
                            .map_err(PeerDriverError::Roster)?;
                        if rejected.reason != ConnectError::Overloaded
                            && !matches!(rejected.reason, ConnectError::Discovery(e) if e.retryable())
                        {
                            return Err(PeerDriverError::Connect(rejected.reason));
                        }
                        out.connection_failures += 1;
                    }
                }
                self.check_connector()?;
            }
        }
        Ok(())
    }
    fn poll_peer_io<O: OutboundQueue>(
        &mut self,
        outbound: &mut O,
        visits: usize,
        out: &mut PeerDriverProgress,
    ) -> Result<(), PeerDriverError> {
        // Complete transport-owned output before admitting more outbound work.
        let mut next_receive = None;
        for _ in 0..visits.min(self.peers.len()) {
            let peer = self.peers[self.cursor];
            self.cursor = (self.cursor + 1) % self.peers.len();
            match self.parts.roster.take_send(peer) {
                Ok(Some(send)) => {
                    let ticket = send.batch.ticket;
                    match outbound.complete(send.batch, send.result) {
                        Ok(done) => {
                            if done.ticket != ticket || done.result != send.result {
                                return Err(PeerDriverError::ProviderViolation);
                            }
                            out.completions.push(done);
                        }
                        Err(rejected) => {
                            self.failed_send = Some(Box::new(TransportSend {
                                connection: send.connection,
                                batch: *rejected.batch,
                                result: send.result,
                            }));
                            return Err(PeerDriverError::Outbound(rejected.reason));
                        }
                    }
                }
                Ok(None) => (),
                Err(rejected) => {
                    self.failed_send = Some(rejected.send);
                    return Err(PeerDriverError::Roster(rejected.reason));
                }
            }
            if !self.closed {
                match self.parts.ingress.receive(&mut self.parts.roster, peer) {
                    Ok(Some(_)) => {
                        out.received += 1;
                        next_receive = Some(self.cursor);
                    }
                    Ok(None) => (),
                    Err(rejected)
                        if rejected.reason == IngressError::Overloaded
                            && rejected.batch.is_none() =>
                    {
                        out.ingress_blocked += 1
                    }
                    Err(rejected) => {
                        self.failed_receive = rejected.batch;
                        return Err(PeerDriverError::Ingress(rejected.reason));
                    }
                }
            }
        }
        // Dispatch may free the same shared credit before the next poll. Resume
        // after its last recipient, not after peers denied by that full budget.
        if let Some(cursor) = next_receive {
            self.cursor = cursor;
        }
        Ok(())
    }
    fn submit_sends<O: OutboundQueue>(
        &mut self,
        outbound: &mut O,
        limit: usize,
        out: &mut PeerDriverProgress,
    ) -> Result<(), PeerDriverError> {
        for _ in 0..limit {
            if self.staged.len() == self.limits.staged_batches {
                break;
            }
            let mut batches = outbound.poll(1);
            if batches.len() > 1 {
                self.quarantine = batches;
                return Err(PeerDriverError::ProviderViolation);
            }
            let Some(batch) = batches.pop() else {
                break;
            };
            self.staged.push_back(batch);
        }
        for _ in 0..self.staged.len().min(limit) {
            let batch = self.staged.pop_front().unwrap();
            if self.closed
                || self.parts.roster.peer_identity(batch.ticket.peer).is_none()
                || self
                    .parts
                    .roster
                    .disconnected_send_deadline(
                        batch.ticket.peer,
                        self.limits.disconnected_send_retry_ms,
                    )
                    .is_some_and(|deadline| self.now >= deadline)
            {
                let ticket = batch.ticket;
                match outbound.complete(batch, LocalSendResult::Failed) {
                    Ok(done) => {
                        if done.ticket != ticket || done.result != LocalSendResult::Failed {
                            return Err(PeerDriverError::ProviderViolation);
                        }
                        out.completions.push(done);
                    }
                    Err(rejected) => {
                        self.staged.push_front(*rejected.batch);
                        return Err(PeerDriverError::Outbound(rejected.reason));
                    }
                }
            } else {
                match self.parts.roster.submit(batch) {
                    Ok(()) => out.sends += 1,
                    Err(rejected) => {
                        self.staged.push_back(*rejected.batch);
                        match rejected.reason {
                            PeerRosterError::Overloaded
                            | PeerRosterError::Transport(TransportError::Overloaded) => (),
                            PeerRosterError::Transport(TransportError::Closed) => {
                                out.connection_failures += 1;
                            }
                            reason => return Err(PeerDriverError::Roster(reason)),
                        }
                    }
                }
            }
        }
        Ok(())
    }
    fn check_connector(&self) -> Result<(), PeerDriverError> {
        let u = self.parts.connector.usage();
        if self.parts.connector.limits() != self.connector_limits
            || u.requests > self.connector_limits.requests
            || u.anonymous > self.connector_limits.anonymous
            || u.dialing > u.requests
            || u.handshaking > u.requests - u.dialing
        {
            return Err(PeerDriverError::ProviderViolation);
        }
        Ok(())
    }
    /// Stop connection/receive admission, drain accepted transport sends, and
    /// resolve retained unsent batches as Failed (unknown remote delivery).
    /// Host-owned outbound queues and serialized owners remain open.
    pub fn close(&mut self) {
        self.closed = true;
        self.parts.connector.close();
        self.parts.roster.close();
        self.parts.ingress.close();
    }
    pub fn into_parts(self) -> Result<PeerParts<C, F>, Box<Self>> {
        if self.is_drained() {
            Ok(self.parts)
        } else {
            Err(Box::new(self))
        }
    }
    /// Consumes a failed driver without claiming provider drain or releasing
    /// queue credits. The host receives every still-observed payload and scope.
    pub fn into_recovery(self) -> Result<PeerDriverRecovery<C, F>, Box<Self>> {
        if self.failed.is_none() {
            return Err(Box::new(self));
        }
        Ok(PeerDriverRecovery {
            parts: self.parts,
            attempts: self.attempts,
            staged: self.staged,
            quarantined: self.quarantine,
            failed_send: self.failed_send,
            failed_receive: self.failed_receive,
        })
    }
}
