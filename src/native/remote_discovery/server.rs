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
use super::{
    channel::Channel,
    codec::{Frame, Payload},
    *,
};
use crate::{discovery::*, runtime::MonoTime, secure::*};
/// Bounded responder over one dedicated session and an independently owned view.
pub struct NativeDiscoveryResponder<S, R> {
    channel: Channel<S>,
    source: R,
    config: RemoteDiscoveryConfig,
    sequence: u64,
    now: MonoTime,
    closed: bool,
}
impl<S: SecureSession, R: PeerDiscovery> NativeDiscoveryResponder<S, R> {
    pub fn new(
        session: S,
        client: PeerIdentity,
        source: R,
        config: RemoteDiscoveryConfig,
        now: MonoTime,
    ) -> Result<Self, (RemoteDiscoveryError, S, R)> {
        if let Err(e) = config.validate() {
            return Err((e, session, source));
        }
        let channel = match Channel::new(session, client) {
            Ok(c) => c,
            Err((e, s)) => return Err((e, s, source)),
        };
        Ok(Self {
            channel,
            source,
            config,
            sequence: 0,
            now,
            closed: false,
        })
    }
    pub fn source_mut(&mut self) -> &mut R {
        &mut self.source
    }
    /// Source views are returned unchanged; stopping a connection does not close them.
    pub fn into_parts(self) -> (S, R) {
        (self.channel.into_session(), self.source)
    }
    pub fn close(&mut self) {
        self.closed = true;
        self.channel.close();
    }
    pub fn poll(
        &mut self,
        now: MonoTime,
        budget: SessionPollBudget,
    ) -> Result<bool, RemoteDiscoveryError> {
        budget.validate()?;
        if now < self.now {
            return Err(DiscoveryError::TimeWentBack.into());
        }
        self.now = now;
        if self.closed {
            return Err(DiscoveryError::Closed.into());
        }
        match self.advance(now, budget) {
            Ok(value) => Ok(value),
            Err(e) => {
                self.close();
                Err(e)
            }
        }
    }
    fn advance(
        &mut self,
        now: MonoTime,
        budget: SessionPollBudget,
    ) -> Result<bool, RemoteDiscoveryError> {
        let Some(frame) = self.channel.poll(now, budget)? else {
            return Ok(false);
        };
        if frame.payload != Payload::Request
            || frame.sequence <= self.sequence
            || self.channel.pending_output()
        {
            return Err(RemoteDiscoveryError::Protocol);
        }
        self.sequence = frame.sequence;
        let payload = match self.source.resolve(frame.peer, now) {
            Ok(hint) => {
                hint.validate(frame.peer, now)?;
                Payload::Hint {
                    generation: hint.generation,
                    endpoint: hint.endpoint,
                    lifetime_ms: (hint.expires_at.0 - now.0).min(self.config.max_lifetime_ms),
                }
            }
            Err(DiscoveryError::Missing | DiscoveryError::Expired) => Payload::Missing,
            Err(_) => Payload::Unavailable,
        };
        self.channel.queue(Frame { payload, ..frame })?;
        Ok(true)
    }
}
