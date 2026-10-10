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
use crate::{discovery::*, native::discovery::NativePeerDiscovery, runtime::MonoTime, secure::*};

/// One request at a time over a dedicated authenticated session. Poll explicitly.
pub struct NativeRemotePeerDiscovery<S> {
    channel: Channel<S>,
    cache: NativePeerDiscovery,
    config: RemoteDiscoveryConfig,
    pending: Option<RefreshRequest>,
    cancelled: bool,
    negative: Option<(PeerIdentity, DiscoveryError, MonoTime)>,
    sequence: u64,
    now: MonoTime,
    closed: bool,
    failed: bool,
}
impl<S: SecureSession> NativeRemotePeerDiscovery<S> {
    pub fn new(
        session: S,
        source: PeerIdentity,
        config: RemoteDiscoveryConfig,
        now: MonoTime,
    ) -> Result<Self, (RemoteDiscoveryError, S)> {
        if let Err(e) = config.validate() {
            return Err((e, session));
        }
        let cache = NativePeerDiscovery::new(config.cached_peers, now).expect("validated capacity");
        let channel = Channel::new(session, source)?;
        Ok(Self {
            channel,
            cache,
            config,
            pending: None,
            cancelled: false,
            negative: None,
            sequence: 0,
            now,
            closed: false,
            failed: false,
        })
    }
    pub fn pending(&self) -> Option<RefreshRequest> {
        self.pending
    }
    pub fn next_deadline(&self) -> Option<MonoTime> {
        self.pending.map(|r| r.deadline)
    }
    pub fn source_failed(&self) -> bool {
        self.failed
    }
    /// Replace a failed connection explicitly, retaining cache floors. The new
    /// authenticated session must have a later generation in the same local
    /// recovered store and authenticate the same provisioned source identity.
    pub fn replace_session(&mut self, session: S) -> Result<S, (RemoteDiscoveryError, S)> {
        if self.closed || !self.failed || self.pending.is_some() {
            return Err((DiscoveryError::Unavailable.into(), session));
        }
        let old = self.channel.binding;
        let source = PeerIdentity {
            node: old.peer.node,
            store: old.peer.store.identity,
        };
        let channel = Channel::new(session, source)?;
        if channel.binding.local != old.local || channel.binding.generation <= old.generation {
            return Err((DiscoveryError::WrongBinding.into(), channel.into_session()));
        }
        let old = std::mem::replace(&mut self.channel, channel);
        self.sequence = 0;
        self.negative = None;
        self.failed = false;
        Ok(old.into_session())
    }
    /// Keep the accepted slot until its response or deadline; suppress publication.
    pub fn cancel(&mut self, request: RefreshRequest) -> bool {
        if self.pending != Some(request) {
            return false;
        }
        self.cancelled = true;
        true
    }
    pub fn into_session(self) -> Result<S, Box<Self>> {
        if (self.closed || self.failed) && self.pending.is_none() {
            Ok(self.channel.into_session())
        } else {
            Err(Box::new(self))
        }
    }
    fn time(&mut self, now: MonoTime) -> Result<(), DiscoveryError> {
        if now < self.now {
            return Err(DiscoveryError::TimeWentBack);
        }
        self.now = now;
        Ok(())
    }
    pub fn poll(
        &mut self,
        now: MonoTime,
        budget: SessionPollBudget,
    ) -> Result<Option<RefreshCompletion>, RemoteDiscoveryError> {
        budget.validate()?;
        self.time(now)?;
        if self.closed {
            return Ok(self.finish(Err(DiscoveryError::Closed.into())));
        }
        if self.failed {
            return Ok(None);
        }
        if self.pending.is_some_and(|r| now >= r.deadline) {
            self.fail_source();
            return Ok(self.finish(Err(RemoteDiscoveryError::Timeout)));
        }
        let frame = match self.channel.poll(now, budget) {
            Ok(frame) => frame,
            Err(e) => {
                self.fail_source();
                return self.failure(e);
            }
        };
        let Some(frame) = frame else {
            return Ok(None);
        };
        let Some(request) = self.pending else {
            self.fail_source();
            return Err(RemoteDiscoveryError::Protocol);
        };
        if frame.sequence != request.sequence
            || frame.peer != request.peer
            || frame.payload == Payload::Request
            || self.channel.pending_output()
        {
            self.fail_source();
            return self.failure(RemoteDiscoveryError::Protocol);
        }
        let result = if self.cancelled {
            Err(RemoteDiscoveryError::Cancelled)
        } else {
            self.observe(request, frame.payload, now)
        };
        if result == Err(RemoteDiscoveryError::Protocol) {
            self.fail_source();
        }
        Ok(self.finish(result))
    }
    fn fail_source(&mut self) {
        self.failed = true;
        self.channel.close();
    }
    fn failure(
        &mut self,
        error: RemoteDiscoveryError,
    ) -> Result<Option<RefreshCompletion>, RemoteDiscoveryError> {
        match self.finish(Err(error)) {
            Some(c) => Ok(Some(c)),
            None => Err(error),
        }
    }
    fn observe(
        &mut self,
        request: RefreshRequest,
        payload: Payload,
        now: MonoTime,
    ) -> Result<PeerEndpointHint, RemoteDiscoveryError> {
        let (generation, endpoint, lifetime_ms) = match payload {
            Payload::Missing => return Err(DiscoveryError::Missing.into()),
            Payload::Unavailable => return Err(DiscoveryError::Unavailable.into()),
            Payload::Hint {
                generation,
                endpoint,
                lifetime_ms,
            } => (generation, endpoint, lifetime_ms),
            Payload::Request => return Err(RemoteDiscoveryError::Protocol),
        };
        if lifetime_ms == 0 || lifetime_ms > self.config.max_lifetime_ms {
            return Err(RemoteDiscoveryError::Protocol);
        }
        let expires_at = MonoTime(
            request
                .started_at
                .0
                .checked_add(lifetime_ms)
                .ok_or(RemoteDiscoveryError::Exhausted)?,
        );
        let hint = PeerEndpointHint {
            peer: request.peer,
            generation,
            endpoint,
            expires_at,
        };
        self.cache
            .revalidate(hint, now)
            .map_err(|(e, _)| RemoteDiscoveryError::Discovery(e))?;
        Ok(hint)
    }
    fn finish(
        &mut self,
        result: Result<PeerEndpointHint, RemoteDiscoveryError>,
    ) -> Option<RefreshCompletion> {
        let request = self.pending.take()?;
        if let Err(error) = result {
            let error = match error {
                RemoteDiscoveryError::Discovery(e) => e,
                _ => DiscoveryError::Unavailable,
            };
            self.negative = Some((
                request.peer,
                error,
                MonoTime(self.now.0.saturating_add(self.config.retry_ms)),
            ));
        } else {
            self.negative = None;
        }
        self.cancelled = false;
        Some(RefreshCompletion { request, result })
    }
}
impl<S: SecureSession> PeerDiscovery for NativeRemotePeerDiscovery<S> {
    fn resolve(
        &mut self,
        peer: PeerIdentity,
        now: MonoTime,
    ) -> Result<PeerEndpointHint, DiscoveryError> {
        self.time(now)?;
        if self.closed {
            return Err(DiscoveryError::Closed);
        }
        match self.cache.resolve(peer, now) {
            Ok(hint) => return Ok(hint),
            Err(DiscoveryError::Missing | DiscoveryError::Expired) => (),
            Err(e) => return Err(e),
        }
        if self.failed {
            return Err(DiscoveryError::Unavailable);
        }
        if let Some((failed, error, retry)) = self.negative {
            if failed == peer && now < retry {
                return Err(error);
            }
        }
        if let Some(request) = self.pending {
            return Err(if request.peer == peer {
                DiscoveryError::Unavailable
            } else {
                DiscoveryError::Overloaded
            });
        }
        let sequence = self
            .sequence
            .checked_add(1)
            .ok_or(DiscoveryError::Unavailable)?;
        let deadline = MonoTime(
            now.0
                .checked_add(self.config.timeout_ms)
                .ok_or(DiscoveryError::Unavailable)?,
        );
        self.channel
            .queue(Frame {
                sequence,
                peer,
                payload: Payload::Request,
            })
            .map_err(|_| DiscoveryError::Unavailable)?;
        self.sequence = sequence;
        self.cancelled = false;
        self.pending = Some(RefreshRequest {
            binding: self.channel.binding,
            sequence,
            peer,
            started_at: now,
            deadline,
        });
        Err(DiscoveryError::Unavailable)
    }
    fn invalidate(&mut self, peer: PeerIdentity, generation: HintGeneration) -> bool {
        self.cache.invalidate(peer, generation)
    }
    fn close(&mut self) {
        self.closed = true;
        self.cache.close();
        self.channel.close();
    }
}

impl<S: SecureSession> DiscoveryDriver for NativeRemotePeerDiscovery<S> {
    fn poll_discovery(
        &mut self,
        now: MonoTime,
        budget: SessionPollBudget,
    ) -> Result<(), DiscoveryError> {
        budget
            .validate()
            .map_err(|_| DiscoveryError::InvalidLimits)?;
        if now < self.now {
            return Err(DiscoveryError::TimeWentBack);
        }
        // poll records negative replies and fences failed sources itself. Such
        // failures must not fence the owning Node or discard unrelated hints.
        let _ = self.poll(now, budget);
        Ok(())
    }
    fn discovery_pending(&self) -> bool {
        self.pending.is_some()
    }
    fn discovery_deadline(&self) -> Option<MonoTime> {
        let pending = self.pending.map(|request| {
            if self.closed {
                self.now
            } else {
                request.deadline
            }
        });
        let retry = if self.closed || self.failed {
            None
        } else {
            self.negative.map(|(_, _, deadline)| deadline)
        };
        pending.or(retry)
    }
}
