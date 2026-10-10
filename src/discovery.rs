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
//! Non-authoritative, bounded peer endpoint hints.
use crate::{
    runtime::MonoTime,
    secure::{PeerIdentity, SessionPollBudget},
};
use std::{net::SocketAddr, num::NonZeroU64};
pub const PEER_DISCOVERY_CONTRACT_VERSION: u32 = 1;
pub const DISCOVERY_DRIVER_CONTRACT_VERSION: u32 = 1;
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct HintGeneration(NonZeroU64);
impl HintGeneration {
    pub fn new(value: u64) -> Option<Self> {
        NonZeroU64::new(value).map(Self)
    }
    pub fn get(self) -> u64 {
        self.0.get()
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PeerEndpointHint {
    pub peer: PeerIdentity,
    pub generation: HintGeneration,
    pub endpoint: SocketAddr,
    pub expires_at: MonoTime,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiscoveryError {
    InvalidLimits,
    InvalidHint,
    Missing,
    Expired,
    Unavailable,
    Overloaded,
    StaleGeneration,
    ConflictingGeneration,
    WrongBinding,
    TimeWentBack,
    Closed,
}
impl DiscoveryError {
    pub fn retryable(self) -> bool {
        matches!(
            self,
            Self::Missing | Self::Expired | Self::Unavailable | Self::Overloaded
        )
    }
}
impl PeerEndpointHint {
    pub fn validate(self, peer: PeerIdentity, now: MonoTime) -> Result<Self, DiscoveryError> {
        if self.peer != peer {
            return Err(DiscoveryError::WrongBinding);
        }
        if self.endpoint.port() == 0
            || self.endpoint.ip().is_unspecified()
            || self.endpoint.ip().is_multicast()
        {
            return Err(DiscoveryError::InvalidHint);
        }
        if now >= self.expires_at {
            return Err(DiscoveryError::Expired);
        }
        Ok(self)
    }
}
/// Synchronous bounded nonblocking lookup. Network/DNS refresh belongs to an
/// explicitly driven host outside this call. Hints cannot provision trust,
/// activate membership/ownership, or establish leadership/read authority.
/// Invalidation affects only the exact generation, never a newer publication.
/// A closed view rejects resolution and does not stop unrelated host resources.
pub trait PeerDiscovery {
    fn resolve(
        &mut self,
        peer: PeerIdentity,
        now: MonoTime,
    ) -> Result<PeerEndpointHint, DiscoveryError>;
    fn invalidate(&mut self, peer: PeerIdentity, generation: HintGeneration) -> bool;
    fn close(&mut self);
}

/// Optional owned progress for asynchronous endpoint lookup. A driven connector
/// is the sole progress/completion consumer; resolve still only admits bounded
/// work. One poll is one visit with the supplied I/O budget, never blocking.
/// Lookup/source failures stay in the provider's per-request/cache state so
/// unrelated live hints remain usable. Errors here mean a progress-contract
/// failure (for example invalid budget/time), not an ordinary failed lookup.
/// Close must suppress publication and retain pending work until terminal poll.
/// Deadlines and pending state remain accurate while closing; no hidden worker
/// or infrastructure owner is created by selecting this extension.
pub trait DiscoveryDriver: PeerDiscovery {
    fn poll_discovery(
        &mut self,
        now: MonoTime,
        budget: SessionPollBudget,
    ) -> Result<(), DiscoveryError>;
    fn discovery_pending(&self) -> bool;
    fn discovery_deadline(&self) -> Option<MonoTime>;
}

impl<T: PeerDiscovery + ?Sized> PeerDiscovery for Box<T> {
    fn resolve(
        &mut self,
        peer: PeerIdentity,
        now: MonoTime,
    ) -> Result<PeerEndpointHint, DiscoveryError> {
        (**self).resolve(peer, now)
    }
    fn invalidate(&mut self, peer: PeerIdentity, generation: HintGeneration) -> bool {
        (**self).invalidate(peer, generation)
    }
    fn close(&mut self) {
        (**self).close();
    }
}
impl<T: DiscoveryDriver + ?Sized> DiscoveryDriver for Box<T> {
    fn poll_discovery(
        &mut self,
        now: MonoTime,
        budget: SessionPollBudget,
    ) -> Result<(), DiscoveryError> {
        (**self).poll_discovery(now, budget)
    }
    fn discovery_pending(&self) -> bool {
        (**self).discovery_pending()
    }
    fn discovery_deadline(&self) -> Option<MonoTime> {
        (**self).discovery_deadline()
    }
}
