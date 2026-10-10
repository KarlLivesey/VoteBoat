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
use crate::{identity::GroupIdentity, routing::*, runtime::MonoTime, secure::*};
struct Pending {
    frame: Frame,
    deadline: MonoTime,
}
/// One provisioned client may read one provisioned authority on this session.
/// The selected view must contain authorized observations, not arbitrary hints.
/// Poll its underlying Node explicitly through source_mut before polling here.
pub struct NativeManifestResponder<S, D> {
    channel: Channel<S>,
    source: D,
    authority: GroupIdentity,
    config: RemoteManifestConfig,
    pending: Option<Pending>,
    sequence: u64,
    now: MonoTime,
    closed: bool,
}
impl<S: SecureSession, D: ManifestDiscovery> NativeManifestResponder<S, D> {
    pub fn new(
        session: S,
        client: PeerIdentity,
        authority: GroupIdentity,
        source: D,
        config: RemoteManifestConfig,
        now: MonoTime,
    ) -> Result<Self, (RemoteManifestError, S, D)> {
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
            authority,
            config,
            pending: None,
            sequence: 0,
            now,
            closed: false,
        })
    }
    pub fn source_mut(&mut self) -> &mut D {
        &mut self.source
    }
    pub fn is_closed(&self) -> bool {
        self.closed
    }
    /// Close only this selected view. Its original accepted Node read remains
    /// owned by the returned source and must still be drained/recovered by host.
    pub fn close(&mut self) {
        self.closed = true;
        self.channel.close();
        self.source.close();
    }
    pub fn into_parts(self) -> (S, D) {
        (self.channel.into_session(), self.source)
    }
    pub fn poll(
        &mut self,
        now: MonoTime,
        budget: SessionPollBudget,
    ) -> Result<bool, RemoteManifestError> {
        budget.validate()?;
        if now < self.now {
            return Err(ManifestDiscoveryError::TimeWentBack.into());
        }
        self.now = now;
        if self.closed {
            return Err(ManifestDiscoveryError::Closed.into());
        }
        let result = self.advance(now, budget);
        if result.is_err() {
            self.close();
        }
        result
    }
    fn advance(
        &mut self,
        now: MonoTime,
        budget: SessionPollBudget,
    ) -> Result<bool, RemoteManifestError> {
        let mut progress = false;
        if let Some(frame) = self.channel.poll(now, budget)? {
            if !matches!(frame.payload, Payload::Request)
                || frame.sequence <= self.sequence
                || self.pending.is_some()
                || self.channel.pending_output()
                || frame.query.locator.authority != self.authority
            {
                return Err(RemoteManifestError::Protocol);
            }
            let deadline = MonoTime(
                now.0
                    .checked_add(self.config.timeout_ms)
                    .ok_or(ManifestDiscoveryError::Exhausted)?,
            );
            self.sequence = frame.sequence;
            self.pending = Some(Pending { frame, deadline });
            progress = true;
        }
        let Some(pending) = &self.pending else {
            return Ok(progress);
        };
        if now >= pending.deadline {
            return Err(RemoteManifestError::Timeout);
        }
        let payload = match self.source.lookup(pending.frame.query, now) {
            Ok(observed) => {
                if observed.locator != pending.frame.query.locator || observed.expires_at <= now {
                    return Err(ManifestDiscoveryError::ProviderViolation.into());
                }
                pending.frame.query.check(&observed.manifest)?;
                Payload::Hint {
                    manifest: Box::new(observed.manifest),
                    lifetime_ms: (observed.expires_at.0 - now.0).min(self.config.max_lifetime_ms),
                }
            }
            Err(ManifestDiscoveryError::Unavailable) => return Ok(progress),
            Err(ManifestDiscoveryError::Missing) => Payload::Missing,
            Err(
                ManifestDiscoveryError::ProviderViolation
                | ManifestDiscoveryError::WrongAuthority
                | ManifestDiscoveryError::WrongIdentity,
            ) => return Err(ManifestDiscoveryError::ProviderViolation.into()),
            Err(_) => Payload::Unavailable,
        };
        let pending = self.pending.take().unwrap();
        self.channel.queue(Frame {
            payload,
            ..pending.frame
        })?;
        Ok(true)
    }
}
