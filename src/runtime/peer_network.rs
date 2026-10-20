// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Construction-selected physical peer networking with original owned lifetimes.
use super::*;
use crate::{connect::ConnectDirection, outbound::*, secure::*, transport::*};
mod configuration;
pub use configuration::*;
mod optional;
pub use optional::*;

pub const PEER_NETWORK_CONTRACT_VERSION: u32 = 1;
pub type PeerNetworkRoutes<N> = BTreeMap<NodeId, PeerRoute<<N as PeerNetwork>::Endpoint>>;

pub struct PeerNetworkRejected<P> {
    pub reason: PeerDriverError,
    pub parts: Box<P>,
}

/// Pure construction description. Catalog identity never attests a live source
/// session, membership activation, readiness or a durable voter response.
pub trait PeerNetworkParts: Sized {
    type Network: PeerNetwork<Parts = Self>;
    fn local(&self) -> LocalIdentity;
    fn original_wire_version(&self) -> u16;
    fn source_identity(&self, source: NodeId) -> Option<PeerIdentity>;
    fn initial_routes(&self) -> Result<PeerNetworkRoutes<Self::Network>, PeerDriverError>
    where
        <Self::Network as PeerNetwork>::Endpoint: Clone;
    fn route_budget(
        &self,
        routes: &PeerNetworkRoutes<Self::Network>,
        limits: PeerDriverLimits,
    ) -> Result<ConnectionBudget, PeerDriverError>;
}

/// One explicitly supplied network owner. Poll serializes original ingress
/// against the effect owner; no provider may create consensus authority.
/// Local sends release queue ownership only, not remote or durable evidence.
/// Closure/recovery must retain every accepted original and unknown outcome.
pub trait PeerNetwork: Sized {
    type Endpoint;
    type Parts: PeerNetworkParts<Network = Self>;
    type Recovery;
    fn build<O: OutboundQueue>(
        parts: Self::Parts,
        owner: RuntimeOwner,
        outbound: &O,
        limits: PeerDriverLimits,
        now: MonoTime,
    ) -> Result<Self, PeerNetworkRejected<Self::Parts>>;
    fn source_view(&self, source: NodeId) -> Option<AuthenticatedSourceView>;
    fn poll<O: OutboundQueue, Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &mut self,
        owner: &mut EffectOwner<Q, T, E>,
        outbound: &mut O,
        now: MonoTime,
        budget: PeerDriverBudget,
    ) -> Result<PeerDriverProgress, PeerDriverError>
    where
        Self::Endpoint: Clone;
    /// Same-poll send flush, called by the node after its replica step in the
    /// same node poll as `poll`: write and complete output queued since that
    /// `poll`, without reading, adding to `progress`. It moves bytes only;
    /// every `Send` reached the queue through the replica's durability gate.
    /// An error has the same effect as a `poll` error. The default does
    /// nothing: queued output then waits for the next `poll`.
    fn flush<O: OutboundQueue, Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &mut self,
        _owner: &mut EffectOwner<Q, T, E>,
        _outbound: &mut O,
        _now: MonoTime,
        _budget: PeerDriverBudget,
        _progress: &mut PeerDriverProgress,
    ) -> Result<(), PeerDriverError> {
        Ok(())
    }
    fn drain<O: OutboundQueue>(
        &mut self,
        outbound: &mut O,
        now: MonoTime,
        budget: PeerDriverBudget,
    ) -> Result<PeerDriverProgress, PeerDriverError>
    where
        Self::Endpoint: Clone;
    fn close(&mut self);
    fn is_drained(&self) -> bool;
    fn next_deadline(&self) -> Option<MonoTime>;
    /// Raw descriptors whose readability means there may be input to poll.
    /// Advisory, for host readiness waits; implementations without sockets
    /// report nothing.
    #[cfg(unix)]
    fn readiness_fds(&self, _out: &mut Vec<crate::worker::ReadinessFd>) {}

    fn into_parts(self) -> Result<Self::Parts, Box<Self>>;
    fn into_recovery(self) -> Result<Self::Recovery, Box<Self>>;
}
