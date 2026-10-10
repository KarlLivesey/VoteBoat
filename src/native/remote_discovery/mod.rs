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
//! Explicitly driven, authenticated remote endpoint hints; no consensus authority.
mod channel;
mod client;
mod codec;
mod reconnect;
mod server;
use crate::{
    discovery::DiscoveryError,
    runtime::MonoTime,
    secure::{PeerIdentity, SessionBinding, SessionError},
};
pub use client::NativeRemotePeerDiscovery;
pub use reconnect::{
    ReconnectingPeerDiscovery, SourceReconnectConfig, SourceReconnectRejected,
    SourceReconnectStatus,
};
pub use server::NativeDiscoveryResponder;

/// Wire version is independent of Raft's wire format. Use a dedicated session.
pub const REMOTE_DISCOVERY_WIRE_VERSION: u8 = 1;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemoteDiscoveryError {
    Discovery(DiscoveryError),
    Session(SessionError),
    Protocol,
    Cancelled,
    Timeout,
    Exhausted,
}
impl From<DiscoveryError> for RemoteDiscoveryError {
    fn from(error: DiscoveryError) -> Self {
        Self::Discovery(error)
    }
}
impl From<SessionError> for RemoteDiscoveryError {
    fn from(error: SessionError) -> Self {
        Self::Session(error)
    }
}
#[derive(Clone, Copy, Debug)]
pub struct RemoteDiscoveryConfig {
    pub cached_peers: usize,
    pub timeout_ms: u64,
    pub retry_ms: u64,
    pub max_lifetime_ms: u64,
}
impl RemoteDiscoveryConfig {
    pub(super) fn validate(self) -> Result<Self, RemoteDiscoveryError> {
        if self.cached_peers == 0
            || self.cached_peers > 1024
            || self.timeout_ms == 0
            || self.timeout_ms > 60_000
            || self.retry_ms == 0
            || self.retry_ms > 60_000
            || self.max_lifetime_ms == 0
            || self.max_lifetime_ms > 3_600_000
        {
            return Err(DiscoveryError::InvalidLimits.into());
        }
        Ok(self)
    }
}
impl Default for RemoteDiscoveryConfig {
    fn default() -> Self {
        Self {
            cached_peers: 64,
            timeout_ms: 10_000,
            retry_ms: 100,
            max_lifetime_ms: 60_000,
        }
    }
}
/// Identity is scoped to one immutable authenticated session, never a Raft receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RefreshRequest {
    pub binding: SessionBinding,
    pub sequence: u64,
    pub peer: PeerIdentity,
    pub started_at: MonoTime,
    pub deadline: MonoTime,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RefreshCompletion {
    pub request: RefreshRequest,
    pub result: Result<crate::discovery::PeerEndpointHint, RemoteDiscoveryError>,
}
