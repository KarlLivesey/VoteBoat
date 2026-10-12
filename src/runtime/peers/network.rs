// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Existing direct peer owner behind the common network contract.
use super::*;

impl<C: PeerConnector, F: PeerTransportFactory<C::Session>> PeerNetworkParts for PeerParts<C, F> {
    type Network = PeerDriver<C, F>;
    fn local(&self) -> LocalIdentity {
        self.roster.local()
    }
    fn original_wire_version(&self) -> u16 {
        self.roster.wire_version()
    }
    fn source_identity(&self, source: NodeId) -> Option<PeerIdentity> {
        self.roster.peer_identity(source)
    }
    fn initial_routes(&self) -> Result<BTreeMap<NodeId, PeerRoute<C::Endpoint>>, PeerDriverError>
    where
        C::Endpoint: Clone,
    {
        if let Some(routes) = &self.admission_routes {
            return Ok(routes.clone());
        }
        self.routes
            .iter()
            .map(|(&node, direction)| {
                let peer = self
                    .roster
                    .peer_identity(node)
                    .ok_or(PeerDriverError::WrongBinding)?;
                Ok((
                    node,
                    PeerRoute {
                        store: peer.store,
                        direction: direction.clone(),
                    },
                ))
            })
            .collect()
    }
    fn route_budget(
        &self,
        routes: &BTreeMap<NodeId, PeerRoute<C::Endpoint>>,
        limits: PeerDriverLimits,
    ) -> Result<ConnectionBudget, PeerDriverError> {
        PeerDriver::route_budget(self, routes, limits)
    }
}

impl<C: PeerConnector, F: PeerTransportFactory<C::Session>> PeerNetwork for PeerDriver<C, F> {
    type Endpoint = C::Endpoint;
    type Parts = PeerParts<C, F>;
    type Recovery = PeerDriverRecovery<C, F>;
    fn build<O: OutboundQueue>(
        parts: Self::Parts,
        owner: RuntimeOwner,
        outbound: &O,
        limits: PeerDriverLimits,
        now: MonoTime,
    ) -> Result<Self, PeerNetworkRejected<Self::Parts>> {
        PeerDriver::new(parts, owner, outbound, limits, now).map_err(|r| PeerNetworkRejected {
            reason: r.reason,
            parts: r.parts,
        })
    }
    fn source_view(&self, source: NodeId) -> Option<AuthenticatedSourceView> {
        self.roster().source_view(source)
    }
    fn poll<O: OutboundQueue, Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &mut self,
        owner: &mut EffectOwner<Q, T, E>,
        outbound: &mut O,
        now: MonoTime,
        budget: PeerDriverBudget,
    ) -> Result<PeerDriverProgress, PeerDriverError>
    where
        C::Endpoint: Clone,
    {
        PeerDriver::poll(self, owner, outbound, now, budget)
    }
    fn drain<O: OutboundQueue>(
        &mut self,
        outbound: &mut O,
        now: MonoTime,
        budget: PeerDriverBudget,
    ) -> Result<PeerDriverProgress, PeerDriverError>
    where
        C::Endpoint: Clone,
    {
        PeerDriver::drain(self, outbound, now, budget)
    }
    fn close(&mut self) {
        PeerDriver::close(self);
    }
    fn is_drained(&self) -> bool {
        PeerDriver::is_drained(self)
    }
    fn next_deadline(&self) -> Option<MonoTime> {
        PeerDriver::next_deadline(self)
    }
    fn into_parts(self) -> Result<Self::Parts, Box<Self>> {
        PeerDriver::into_parts(self)
    }
    fn into_recovery(self) -> Result<Self::Recovery, Box<Self>> {
        PeerDriver::into_recovery(self)
    }
}

impl<C: PeerConnector, F: PeerTransportFactory<C::Session>> PeerNetworkConfiguration
    for PeerDriver<C, F>
{
    fn original_wire_version(&self) -> u16 {
        self.roster().wire_version()
    }
    fn install_admission_routes(&mut self, routes: BTreeMap<NodeId, PeerRoute<C::Endpoint>>) {
        PeerDriver::install_admission_routes(self, routes);
    }
    fn configuration_capacity(
        &self,
        core: &Raft,
        proposal: &ConfigurationProposal,
    ) -> Result<(), TransportError> {
        PeerDriver::configuration_capacity(self, core, proposal).map(|_| ())
    }
    fn reconcile_planned_membership<
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
        PeerDriver::reconcile_planned_membership(self, owner, outbound, now)
    }
    fn preflight_event<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &self,
        owner: &EffectOwner<Q, T, E>,
        group: GroupIdentity,
        event: &Event,
        routes: &BTreeMap<NodeId, ConnectDirection<C::Endpoint>>,
        now: MonoTime,
    ) -> Result<(), PeerDriverError>
    where
        C::Endpoint: Clone,
    {
        PeerDriver::preflight_event(self, owner, group, event, routes, now)
    }
    fn reconcile_membership<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &mut self,
        owner: &EffectOwner<Q, T, E>,
        routes: BTreeMap<NodeId, ConnectDirection<C::Endpoint>>,
        now: MonoTime,
    ) -> Result<(), PeerReconcileRejected<C::Endpoint>>
    where
        C::Endpoint: Clone,
    {
        PeerDriver::reconcile_membership(self, owner, routes, now)
    }
    fn set_admission_routes<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &mut self,
        owner: &mut EffectOwner<Q, T, E>,
        routes: BTreeMap<NodeId, PeerRoute<C::Endpoint>>,
        now: MonoTime,
    ) -> Result<(), PeerRoutesRejected<C::Endpoint>>
    where
        C::Endpoint: Clone,
    {
        PeerDriver::set_admission_routes(self, owner, routes, now)
    }
    fn disconnect(&mut self, source: NodeId, now: MonoTime) -> Result<(), PeerDriverError> {
        PeerDriver::disconnect(self, source, now)
    }
}

impl<C: PeerConnector + PeerCredentialControl, F: PeerTransportFactory<C::Session>>
    PeerNetworkCredentials for PeerDriver<C, F>
{
    type Credentials = C::Credentials;
    fn replace_peer_credentials(
        &mut self,
        expected: crate::authorization::CredentialGeneration,
        replacement: crate::authorization::CredentialGeneration,
        material: Self::Credentials,
    ) -> Result<Self::Credentials, (ConnectError, Self::Credentials)> {
        PeerDriver::replace_peer_credentials(self, expected, replacement, material)
    }
}

impl<C: PeerConnector, F: PeerTransportFactory<C::Session>> PeerNetworkInspection
    for PeerDriver<C, F>
{
    type DrainStatus = PeerDrainStatus;
    fn peer_drain_status(&mut self, source: NodeId) -> Option<Self::DrainStatus> {
        PeerDriver::peer_drain_status(self, source)
    }
}
