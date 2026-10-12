// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Exact route/capacity operations, separate from ordinary data polling.
use super::*;

pub trait PeerNetworkConfiguration: PeerNetwork {
    fn original_wire_version(&self) -> u16;
    fn install_admission_routes(&mut self, routes: BTreeMap<NodeId, PeerRoute<Self::Endpoint>>);
    fn configuration_capacity(
        &self,
        core: &Raft,
        proposal: &ConfigurationProposal,
    ) -> Result<(), TransportError>;
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
        Self::Endpoint: Clone;
    fn preflight_event<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &self,
        owner: &EffectOwner<Q, T, E>,
        group: GroupIdentity,
        event: &Event,
        routes: &BTreeMap<NodeId, ConnectDirection<Self::Endpoint>>,
        now: MonoTime,
    ) -> Result<(), PeerDriverError>
    where
        Self::Endpoint: Clone;
    fn reconcile_membership<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &mut self,
        owner: &EffectOwner<Q, T, E>,
        routes: BTreeMap<NodeId, ConnectDirection<Self::Endpoint>>,
        now: MonoTime,
    ) -> Result<(), PeerReconcileRejected<Self::Endpoint>>
    where
        Self::Endpoint: Clone;
    fn set_admission_routes<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &mut self,
        owner: &mut EffectOwner<Q, T, E>,
        routes: BTreeMap<NodeId, PeerRoute<Self::Endpoint>>,
        now: MonoTime,
    ) -> Result<(), PeerRoutesRejected<Self::Endpoint>>
    where
        Self::Endpoint: Clone;
    fn disconnect(&mut self, source: NodeId, now: MonoTime) -> Result<(), PeerDriverError>;
}
