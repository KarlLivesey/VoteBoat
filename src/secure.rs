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
//! Nonblocking authenticated-channel contract. Host replacements attest their
//! security capability; simulator-only channels cannot enter production assembly.
use crate::{identity::*, runtime::MonoTime, wire::WireScope};
use std::io::ErrorKind;
mod validity;
pub use validity::{GuardedSession, SessionValidity, SESSION_VALIDITY_CONTRACT_VERSION};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalIdentity {
    pub node: NodeId,
    pub store: StoreBinding,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PeerIdentity {
    pub node: NodeId,
    pub store: StoreIdentity,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SessionBinding {
    pub local: LocalIdentity,
    pub peer: LocalIdentity,
    pub generation: SecureSessionGeneration,
    pub wire_version: u16,
}
impl SessionBinding {
    pub fn incoming(self) -> WireScope {
        WireScope {
            from: self.peer.node,
            sender: self.peer.store,
            to: self.local.node,
        }
    }
    pub fn outgoing(self) -> WireScope {
        WireScope {
            from: self.local.node,
            sender: self.local.store,
            to: self.peer.node,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionSecurity {
    Authenticated,
    SimulatorOnly,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionState {
    Handshaking,
    Ready,
    Closing,
    Closed,
    Failed,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionError {
    InvalidLimits,
    InvalidCredentials,
    WrongPeer,
    Authentication,
    IncompatibleProtocol,
    InsecureProvider,
    NotReady,
    WouldBlock,
    Closed,
    Truncated,
    Timeout,
    HandshakeTooLarge,
    Revoked,
    TimeWentBack,
    Io(ErrorKind),
    Failed,
}
#[derive(Clone, Copy, Debug)]
pub struct SessionLimits {
    pub write_buffer_bytes: usize,
    pub handshake_bytes: usize,
    pub handshake_timeout_ms: u64,
}
impl Default for SessionLimits {
    fn default() -> Self {
        Self {
            write_buffer_bytes: 16 * 1024,
            handshake_bytes: 1024 * 1024,
            handshake_timeout_ms: 10_000,
        }
    }
}
impl SessionLimits {
    pub fn validate(self) -> Result<Self, SessionError> {
        if self.write_buffer_bytes < 128
            || self.write_buffer_bytes > 1024 * 1024
            || self.handshake_bytes < 128
            || self.handshake_bytes > 16 * 1024 * 1024
            || self.handshake_timeout_ms == 0
            || self.handshake_timeout_ms > 3_600_000
        {
            return Err(SessionError::InvalidLimits);
        }
        Ok(self)
    }
}
#[derive(Clone, Copy, Debug)]
pub struct SessionPollBudget {
    pub io_calls: usize,
    pub read_bytes: usize,
    pub write_bytes: usize,
}
impl Default for SessionPollBudget {
    fn default() -> Self {
        Self {
            io_calls: 8,
            read_bytes: 64 * 1024,
            write_bytes: 64 * 1024,
        }
    }
}
impl SessionPollBudget {
    pub fn validate(self) -> Result<Self, SessionError> {
        if self.io_calls > 1024
            || self.read_bytes > 16 * 1024 * 1024
            || self.write_bytes > 16 * 1024 * 1024
        {
            return Err(SessionError::InvalidLimits);
        }
        Ok(self)
    }
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SessionProgress {
    pub io_calls: usize,
    pub read_bytes: usize,
    pub written_bytes: usize,
    pub became_ready: bool,
}

/// The provider supplies established authenticated cryptography, mutual peer
/// identity binding and nonblocking plaintext I/O. `binding` appears only after
/// authentication and protocol/store-session exchange. Peer store identity must
/// match construction-time authorization; its recovered session is authenticated
/// channel data, not proof of a remote durable prefix. Generations are fresh per
/// local connection and never reused within the recovered local store session.
///
/// Poll owns bounded external I/O progress. `WouldBlock` retains state and never
/// means rollback. Plaintext writes may accept a short prefix; the transport
/// retains the rest. `is_flushed` means local ciphertext buffers are empty; it
/// does not guarantee remote receipt. Read returns zero only on clean channel EOF; unclean EOF is
/// `Truncated`. Caller-supplied monotonic time drives handshake deadlines.
///
/// The owner serializes calls, supplies reactor wakeups, and budgets plaintext
/// frames/outputs separately. Providers may conservatively wait for transport
/// acknowledgements before reporting flushed; this still promises no remote
/// application consumption or Raft durability. Clean peer closure stops new
/// writes; already decrypted plaintext remains readable after closure. Close
/// stops new writes and flushes accepted channel output; dropping observation
/// cannot undo external progress. Revocation latches
/// failure and prevents further plaintext I/O. Key/certificate rotation creates
/// a new authenticated connection. No method shuts down shared host resources.
pub trait SecureSession {
    fn security(&self) -> SessionSecurity;
    fn state(&self) -> SessionState;
    fn binding(&self) -> Option<SessionBinding>;
    fn limits(&self) -> SessionLimits;
    fn poll(
        &mut self,
        now: MonoTime,
        budget: SessionPollBudget,
    ) -> Result<SessionProgress, SessionError>;
    fn read_plaintext(&mut self, bytes: &mut [u8]) -> Result<usize, SessionError>;
    fn write_plaintext(&mut self, bytes: &[u8]) -> Result<usize, SessionError>;
    fn is_flushed(&self) -> bool;
    fn close(&mut self);
    fn revoke(&mut self);
}
/// Production composition must call this before accepting a channel for Raft
/// traffic; a security capability alone is insufficient until it is ready.
pub fn require_authenticated(
    session: &(impl SecureSession + ?Sized),
) -> Result<SessionBinding, SessionError> {
    if session.security() != SessionSecurity::Authenticated {
        return Err(SessionError::InsecureProvider);
    }
    if session.state() != SessionState::Ready {
        return Err(SessionError::NotReady);
    }
    session.binding().ok_or(SessionError::NotReady)
}

impl<S: SecureSession + ?Sized> SecureSession for Box<S> {
    fn security(&self) -> SessionSecurity {
        (**self).security()
    }
    fn state(&self) -> SessionState {
        (**self).state()
    }
    fn binding(&self) -> Option<SessionBinding> {
        (**self).binding()
    }
    fn limits(&self) -> SessionLimits {
        (**self).limits()
    }
    fn poll(
        &mut self,
        now: MonoTime,
        budget: SessionPollBudget,
    ) -> Result<SessionProgress, SessionError> {
        (**self).poll(now, budget)
    }
    fn read_plaintext(&mut self, bytes: &mut [u8]) -> Result<usize, SessionError> {
        (**self).read_plaintext(bytes)
    }
    fn write_plaintext(&mut self, bytes: &[u8]) -> Result<usize, SessionError> {
        (**self).write_plaintext(bytes)
    }
    fn is_flushed(&self) -> bool {
        (**self).is_flushed()
    }
    fn close(&mut self) {
        (**self).close();
    }
    fn revoke(&mut self) {
        (**self).revoke();
    }
}
