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
//! Bounded asynchronous connection dialing. A connected channel is untrusted:
//! callers must authenticate it before constructing a production PeerTransport.
use crate::{secure::LocalIdentity, transport::ConnectTicket};
use std::io::ErrorKind;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DialLimits {
    /// Includes queued, active and completed-but-unpolled requests/channels.
    pub requests: usize,
    /// Maximum wall-clock timeout for one underlying connect call, excluding
    /// queue delay. End-to-end roster deadlines require explicit cancellation.
    pub connect_timeout_ms: u64,
}
impl Default for DialLimits {
    fn default() -> Self {
        Self {
            requests: 16,
            connect_timeout_ms: 1_000,
        }
    }
}
impl DialLimits {
    pub fn validate(self) -> Result<Self, DialError> {
        if self.requests == 0
            || self.requests > 1024
            || self.connect_timeout_ms == 0
            || self.connect_timeout_ms > 10_000
        {
            return Err(DialError::InvalidLimits);
        }
        Ok(self)
    }
}
#[derive(Debug)]
pub struct DialRequest<E> {
    pub connection: ConnectTicket,
    /// An address hint, never authority to select a peer identity or membership.
    pub endpoint: E,
    pub timeout_ms: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DialError {
    InvalidLimits,
    InvalidRequest,
    WrongBinding,
    UnknownPeer,
    StaleConnection,
    Overloaded,
    Closed,
    Cancelled,
    WorkerFailed,
    Io(ErrorKind),
}
#[derive(Debug)]
pub struct DialRejected<E> {
    pub reason: DialError,
    pub request: Box<DialRequest<E>>,
}
#[derive(Debug)]
pub struct DialCompletion<I> {
    pub connection: ConnectTicket,
    pub result: Result<I, DialError>,
}
/// Construction binds local identity and a finite authorized peer/store map.
/// Submission rejects whole requests before transfer; acceptance retains one
/// slot until exactly one terminal poll. At most one request per peer is live.
/// Accepted generations increase per peer within a recovered local store
/// session; hosts reserve nonoverlapping ranges, as required by PeerRoster.
/// Restart needs a fresh persisted store session, never a reused generation.
///
/// Cancellation identifies the exact ticket and does not release its slot until
/// the provider has stopped using/closed the channel. A provider may need to wait
/// for a bounded OS connect call. Cancelling a queued or completed-but-unpolled
/// request also yields Cancelled. Close stops admission and cancels all work;
/// poll must drain terminal outcomes. Dropping abandons observation and must
/// eventually close retained channels without shutting down shared host resources.
///
/// Poll is nonblocking, returns at most limit completions, and owns no consensus
/// state. Connected channels must support the chosen nonblocking SecureSession
/// provider. No dial result is an authenticated identity or durable evidence.
pub trait PeerDialer {
    type Endpoint;
    type Channel;
    fn local(&self) -> LocalIdentity;
    fn limits(&self) -> DialLimits;
    fn outstanding(&self) -> usize;
    fn submit(
        &mut self,
        request: DialRequest<Self::Endpoint>,
    ) -> Result<(), DialRejected<Self::Endpoint>>;
    fn cancel(&mut self, connection: ConnectTicket) -> bool;
    fn poll(&mut self, limit: usize) -> Vec<DialCompletion<Self::Channel>>;
    fn close(&mut self);
    fn is_drained(&self) -> bool {
        self.outstanding() == 0
    }
}
