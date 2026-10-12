// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Explicit prepared static routes; catalogs never create live source sessions.
use super::*;
use parts::validate_routes;
impl<S: SecureSession, W: WireCodec> NativeRelayNetwork<S, W> {
    fn check_peers(&self, peers: &BTreeMap<NodeId, StoreIdentity>) -> Result<(), PeerDriverError> {
        if peers.len() > self.parts.limits.source_slots
            || peers
                .iter()
                .any(|(&n, &s)| self.parts.plan.source(n).is_none_or(|p| p.store != s))
        {
            return Err(PeerDriverError::WrongBinding);
        }
        Ok(())
    }
    fn check_membership<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &self,
        owner: &EffectOwner<Q, T, E>,
        group: Option<(GroupIdentity, &Event)>,
    ) -> Result<(), PeerDriverError> {
        for g in owner.groups() {
            let core = owner.core(g).unwrap();
            let peers = match group {
                Some((target, e)) if g == target => {
                    core.event_connection_replicas(e, self.parts.limits.source_slots)
                }
                _ => core.connection_replicas(self.parts.limits.source_slots),
            }
            .map_err(|e| PeerDriverError::Owner(EffectOwnerError::Consensus(e)))?;
            self.check_peers(&peers)?;
        }
        Ok(())
    }
}
impl<S: SecureSession, W: WireCodec> PeerNetworkConfiguration for NativeRelayNetwork<S, W> {
    fn original_wire_version(&self) -> u16 {
        self.parts.plan.original_wire_version()
    }
    fn install_admission_routes(&mut self, routes: BTreeMap<NodeId, PeerRoute<RelaySourceRoute>>) {
        self.parts.routes = Some(routes);
    }
    fn configuration_capacity(
        &self,
        core: &Raft,
        proposal: &ConfigurationProposal,
    ) -> Result<(), TransportError> {
        let capacity = self
            .parts
            .codec
            .configuration_capacity(&crate::wire::ConfigurationWireRequirements {
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
            })
            .map_err(TransportError::Wire)?;
        if capacity.wire_version != self.parts.plan.original_wire_version()
            || [capacity.append, capacity.command, capacity.snapshot]
                .iter()
                .any(|f| {
                    f.frame_bytes > self.parts.limits.frame_bytes.saturating_sub(1024)
                        || f.decoded_bytes > self.parts.limits.decoded_bytes
                })
        {
            return Err(TransportError::Wire(WireError::TooLarge));
        }
        Ok(())
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
    ) -> Result<(), PeerDriverError> {
        self.check(owner.identity(), outbound.binding(), now)?;
        self.check_membership(owner, None)
    }
    fn preflight_event<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &self,
        owner: &EffectOwner<Q, T, E>,
        group: GroupIdentity,
        event: &Event,
        routes: &BTreeMap<NodeId, ConnectDirection<RelaySourceRoute>>,
        now: MonoTime,
    ) -> Result<(), PeerDriverError> {
        self.check(owner.identity(), self.outbound, now)?;
        let routes = self.wrap_routes(routes)?;
        validate_routes(&self.parts.plan, &routes)?;
        self.check_membership(owner, Some((group, event)))
    }
    fn reconcile_membership<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &mut self,
        owner: &EffectOwner<Q, T, E>,
        routes: BTreeMap<NodeId, ConnectDirection<RelaySourceRoute>>,
        now: MonoTime,
    ) -> Result<(), PeerReconcileRejected<RelaySourceRoute>> {
        let checked = self
            .check(owner.identity(), self.outbound, now)
            .and_then(|()| self.wrap_routes(&routes))
            .and_then(|r| validate_routes(&self.parts.plan, &r))
            .and_then(|()| self.check_membership(owner, None));
        checked.map_err(|reason| PeerReconcileRejected { reason, routes })
    }
    fn set_admission_routes<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &mut self,
        owner: &mut EffectOwner<Q, T, E>,
        routes: BTreeMap<NodeId, PeerRoute<RelaySourceRoute>>,
        now: MonoTime,
    ) -> Result<(), PeerRoutesRejected<RelaySourceRoute>> {
        let result = self
            .check(owner.identity(), self.outbound, now)
            .and_then(|()| validate_routes(&self.parts.plan, &routes))
            .and_then(|()| self.check_membership(owner, None));
        if let Err(reason) = result {
            return Err(PeerRoutesRejected { reason, routes });
        }
        self.parts.routes = Some(routes);
        Ok(())
    }
    fn disconnect(&mut self, source: NodeId, now: MonoTime) -> Result<(), PeerDriverError> {
        self.check(self.owner, self.outbound, now)?;
        if self.parts.plan.source(source).is_none() || source == self.parts.plan.local().node {
            return Err(PeerDriverError::WrongBinding);
        }
        self.sources.remove(&source);
        Ok(())
    }
}
impl<S: SecureSession, W: WireCodec> NativeRelayNetwork<S, W> {
    fn wrap_routes(
        &self,
        routes: &BTreeMap<NodeId, ConnectDirection<RelaySourceRoute>>,
    ) -> Result<BTreeMap<NodeId, PeerRoute<RelaySourceRoute>>, PeerDriverError> {
        routes
            .iter()
            .map(|(&n, d)| {
                let p = self
                    .parts
                    .plan
                    .source(n)
                    .ok_or(PeerDriverError::WrongBinding)?;
                Ok((
                    n,
                    PeerRoute {
                        store: p.store,
                        direction: d.clone(),
                    },
                ))
            })
            .collect()
    }
}
