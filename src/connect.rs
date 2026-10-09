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
//! Bounded establishment of authenticated peer sessions. Address/routing hints
//! cannot grant authority, and successful establishment is not Raft evidence.
use crate::{dial::DialError, runtime::MonoTime, secure::*, transport::ConnectTicket};
use std::io::ErrorKind;
mod discovery;
pub use discovery::DiscoveryConnector;
#[derive(Clone, Debug)]
pub enum ConnectDirection<E> {
    Dial(E),
    Accept,
}
#[derive(Debug)]
pub struct ConnectRequest<E> {
    pub ticket: ConnectTicket,
    pub direction: ConnectDirection<E>,
    pub deadline: MonoTime,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectError {
    InvalidLimits,
    InvalidRequest,
    WrongBinding,
    UnknownPeer,
    StaleConnection,
    Overloaded,
    Closed,
    Cancelled,
    Timeout,
    TimeWentBack,
    ProviderViolation,
    Dial(DialError),
    Discovery(crate::discovery::DiscoveryError),
    Session(SessionError),
    Io(ErrorKind),
}
#[derive(Debug)]
pub struct ConnectRejected<E> {
    pub reason: ConnectError,
    pub request: Box<ConnectRequest<E>>,
}
#[derive(Debug)]
pub struct ConnectCompletion<S> {
    pub ticket: ConnectTicket,
    pub result: Result<S, ConnectError>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectLimits {
    /// Queued, dialing, handshaking and unpolled terminal attempts combined.
    pub requests: usize,
    /// Separate slots for incoming sockets awaiting fixed routing prefaces.
    pub anonymous: usize,
    /// Maximum caller-supplied end-to-end attempt and anonymous preface timeout.
    pub timeout_ms: u64,
}
impl Default for ConnectLimits {
    fn default() -> Self {
        Self {
            requests: 16,
            anonymous: 16,
            timeout_ms: 10_000,
        }
    }
}
impl ConnectLimits {
    pub fn validate(self) -> Result<Self, ConnectError> {
        if self.requests == 0
            || self.requests > 1024
            || self.anonymous > 1024
            || self.timeout_ms == 0
            || self.timeout_ms > 60_000
        {
            return Err(ConnectError::InvalidLimits);
        }
        Ok(self)
    }
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConnectUsage {
    pub requests: usize,
    pub dialing: usize,
    pub anonymous: usize,
    pub handshaking: usize,
}
#[derive(Clone, Copy, Debug)]
pub struct ConnectPollBudget {
    /// At most this many attempts AND anonymous slots visited once each.
    pub visits: usize,
    /// Combined accept and fixed-preface read/write calls (TLS has its own budget).
    pub socket_calls: usize,
    /// Maximum dial receipts ingested AND terminal sessions returned.
    pub completions: usize,
    /// Per visited handshake; global TLS I/O is bounded by visits * this budget.
    pub session: SessionPollBudget,
}
impl Default for ConnectPollBudget {
    fn default() -> Self {
        Self {
            visits: 16,
            socket_calls: 32,
            completions: 16,
            session: SessionPollBudget::default(),
        }
    }
}
impl ConnectPollBudget {
    pub fn validate(self) -> Result<Self, ConnectError> {
        if self.visits > 1024 || self.socket_calls > 1024 || self.completions > 1024 {
            return Err(ConnectError::InvalidLimits);
        }
        self.session.validate().map_err(ConnectError::Session)?;
        Ok(self)
    }
}
/// A construction-selected provider owns address execution, bounded listener
/// acceptance and authentication. It returns only Ready authenticated sessions
/// whose local/peer/store/generation/wire scope matches the accepted ticket.
/// The caller must revalidate a live roster attempt before attaching a transport.
///
/// Reject whole requests before transfer. Accepted tickets retain slots through
/// exactly one terminal poll, including canceled/expired work still using an OS
/// socket. One request per authorized peer; generations increase per peer in a
/// host-reserved nonoverlapping range within a recovered local store session.
/// Restart needs a fresh persisted session. Deadlines use supplied monotonic time.
/// No callback modifies a Raft core or creates quorum/durability evidence.
///
/// Cancel matches the complete ticket; it closes owned sockets/handshakes but
/// retains active dialing until its actual terminal receipt. Close stops admission,
/// closes listener/anonymous sockets and cancels all attempts. Poll drains accepted
/// outcomes; transferred sessions belong to the caller. Drop closes owned resources
/// and abandons observation. Shared host reactors remain owned by the host. Socket
/// readiness and deadline wake scheduling are host responsibilities.
pub trait PeerConnector {
    /// Host endpoint payloads and clones must be bounded by the embedding host.
    /// Native endpoints are fixed-size SocketAddr values. Generic route metadata
    /// accounting cannot inspect heap allocations inside a host-defined endpoint.
    type Endpoint;
    type Session: SecureSession;
    fn local(&self) -> LocalIdentity;
    fn limits(&self) -> ConnectLimits;
    fn usage(&self) -> ConnectUsage;
    fn next_deadline(&self) -> Option<MonoTime>;
    /// Exact construction-provisioned credential identity, for membership-driven
    /// connection admission. False by default; hints never provision trust.
    fn supports_peer(&self, _peer: PeerIdentity) -> bool {
        false
    }
    fn submit(
        &mut self,
        request: ConnectRequest<Self::Endpoint>,
        now: MonoTime,
    ) -> Result<(), ConnectRejected<Self::Endpoint>>;
    fn cancel(&mut self, ticket: ConnectTicket) -> bool;
    fn poll(
        &mut self,
        now: MonoTime,
        budget: ConnectPollBudget,
    ) -> Result<Vec<ConnectCompletion<Self::Session>>, ConnectError>;
    fn close(&mut self);
    fn is_drained(&self) -> bool {
        self.usage().requests == 0 && self.usage().anonymous == 0
    }
}
