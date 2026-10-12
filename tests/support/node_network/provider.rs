// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Wrapper implements the public network families without private node access.
use super::*;

#[derive(Default)]
pub(super) struct Calls {
    pub builds: Cell<usize>,
    pub polls: Cell<usize>,
    pub closes: Cell<usize>,
    pub reject: Cell<bool>,
    pub substituted_source: Cell<Option<NodeId>>,
}
pub(super) struct WatchedParts<N: PeerNetwork> {
    pub inner: N::Parts,
    pub calls: Rc<Calls>,
}
pub(super) struct Watched<N: PeerNetwork> {
    inner: N,
    calls: Rc<Calls>,
}
impl<N: PeerNetwork> PeerNetworkParts for WatchedParts<N> {
    type Network = Watched<N>;
    fn local(&self) -> LocalIdentity {
        self.inner.local()
    }
    fn original_wire_version(&self) -> u16 {
        self.inner.original_wire_version()
    }
    fn source_identity(&self, n: NodeId) -> Option<PeerIdentity> {
        self.inner.source_identity(n)
    }
    fn initial_routes(&self) -> Result<PeerNetworkRoutes<Self::Network>, PeerDriverError>
    where
        N::Endpoint: Clone,
    {
        self.inner.initial_routes()
    }
    fn route_budget(
        &self,
        r: &PeerNetworkRoutes<Self::Network>,
        l: PeerDriverLimits,
    ) -> Result<ConnectionBudget, PeerDriverError> {
        self.inner.route_budget(r, l)
    }
}
impl<N: PeerNetwork> PeerNetwork for Watched<N> {
    type Endpoint = N::Endpoint;
    type Parts = WatchedParts<N>;
    type Recovery = (N::Recovery, Rc<Calls>);
    fn build<O: OutboundQueue>(
        p: Self::Parts,
        owner: RuntimeOwner,
        out: &O,
        l: PeerDriverLimits,
        now: MonoTime,
    ) -> Result<Self, PeerNetworkRejected<Self::Parts>> {
        p.calls.builds.set(p.calls.builds.get() + 1);
        if p.calls.reject.get() {
            return Err(PeerNetworkRejected {
                reason: PeerDriverError::WrongBinding,
                parts: Box::new(p),
            });
        }
        let WatchedParts { inner, calls } = p;
        match N::build(inner, owner, out, l, now) {
            Ok(inner) => Ok(Self { inner, calls }),
            Err(r) => Err(PeerNetworkRejected {
                reason: r.reason,
                parts: Box::new(WatchedParts {
                    inner: *r.parts,
                    calls,
                }),
            }),
        }
    }
    fn source_view(&self, n: NodeId) -> Option<AuthenticatedSourceView> {
        self.inner
            .source_view(self.calls.substituted_source.get().unwrap_or(n))
    }
    fn poll<O: OutboundQueue, Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &mut self,
        owner: &mut EffectOwner<Q, T, E>,
        out: &mut O,
        now: MonoTime,
        b: PeerDriverBudget,
    ) -> Result<PeerDriverProgress, PeerDriverError>
    where
        N::Endpoint: Clone,
    {
        self.calls.polls.set(self.calls.polls.get() + 1);
        self.inner.poll(owner, out, now, b)
    }
    fn drain<O: OutboundQueue>(
        &mut self,
        out: &mut O,
        now: MonoTime,
        b: PeerDriverBudget,
    ) -> Result<PeerDriverProgress, PeerDriverError>
    where
        N::Endpoint: Clone,
    {
        self.inner.drain(out, now, b)
    }
    fn close(&mut self) {
        self.calls.closes.set(self.calls.closes.get() + 1);
        self.inner.close();
    }
    fn is_drained(&self) -> bool {
        self.inner.is_drained()
    }
    fn next_deadline(&self) -> Option<MonoTime> {
        self.inner.next_deadline()
    }
    fn into_parts(self) -> Result<Self::Parts, Box<Self>> {
        match self.inner.into_parts() {
            Ok(inner) => Ok(WatchedParts {
                inner,
                calls: self.calls,
            }),
            Err(inner) => Err(Box::new(Self {
                inner: *inner,
                calls: self.calls,
            })),
        }
    }
    fn into_recovery(self) -> Result<Self::Recovery, Box<Self>> {
        match self.inner.into_recovery() {
            Ok(inner) => Ok((inner, self.calls)),
            Err(inner) => Err(Box::new(Self {
                inner: *inner,
                calls: self.calls,
            })),
        }
    }
}
impl<N: PeerNetworkConfiguration> PeerNetworkConfiguration for Watched<N> {
    fn original_wire_version(&self) -> u16 {
        self.inner.original_wire_version()
    }
    fn install_admission_routes(&mut self, r: BTreeMap<NodeId, PeerRoute<N::Endpoint>>) {
        self.inner.install_admission_routes(r);
    }
    fn configuration_capacity(
        &self,
        c: &Raft,
        p: &ConfigurationProposal,
    ) -> Result<(), TransportError> {
        self.inner.configuration_capacity(c, p)
    }
    fn reconcile_planned_membership<
        Q: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        O: OutboundQueue,
    >(
        &mut self,
        o: &EffectOwner<Q, T, E>,
        out: &O,
        now: MonoTime,
    ) -> Result<(), PeerDriverError>
    where
        N::Endpoint: Clone,
    {
        self.inner.reconcile_planned_membership(o, out, now)
    }
    fn preflight_event<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &self,
        o: &EffectOwner<Q, T, E>,
        g: GroupIdentity,
        e: &Event,
        r: &BTreeMap<NodeId, ConnectDirection<N::Endpoint>>,
        now: MonoTime,
    ) -> Result<(), PeerDriverError>
    where
        N::Endpoint: Clone,
    {
        self.inner.preflight_event(o, g, e, r, now)
    }
    fn reconcile_membership<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &mut self,
        o: &EffectOwner<Q, T, E>,
        r: BTreeMap<NodeId, ConnectDirection<N::Endpoint>>,
        now: MonoTime,
    ) -> Result<(), PeerReconcileRejected<N::Endpoint>>
    where
        N::Endpoint: Clone,
    {
        self.inner.reconcile_membership(o, r, now)
    }
    fn set_admission_routes<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &mut self,
        o: &mut EffectOwner<Q, T, E>,
        r: BTreeMap<NodeId, PeerRoute<N::Endpoint>>,
        now: MonoTime,
    ) -> Result<(), PeerRoutesRejected<N::Endpoint>>
    where
        N::Endpoint: Clone,
    {
        self.inner.set_admission_routes(o, r, now)
    }
    fn disconnect(&mut self, n: NodeId, now: MonoTime) -> Result<(), PeerDriverError> {
        self.inner.disconnect(n, now)
    }
}
