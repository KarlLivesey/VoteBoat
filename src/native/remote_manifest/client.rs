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
use crate::{identity::*, routing::*, runtime::MonoTime, secure::*};
use std::collections::BTreeMap;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ManifestRefresh {
    pub binding: SessionBinding,
    pub sequence: u64,
    pub query: ManifestLookup,
    pub started_at: MonoTime,
    pub deadline: MonoTime,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ManifestRefreshCompletion {
    pub request: ManifestRefresh,
    pub result: Result<ManifestObservationId, RemoteManifestError>,
}
struct Observation {
    id: ManifestObservationId,
    expires: MonoTime,
    live: bool,
}
/// Remote observations are cache hints, never locally minted ReadBarriers.
/// Host provisions the authenticated source and its allowed authority group.
pub struct NativeRemoteManifestDiscovery<S, C> {
    channel: Channel<S>,
    cache: C,
    authority: GroupIdentity,
    config: RemoteManifestConfig,
    observations: BTreeMap<ResponsibilityIdentity, Observation>,
    pending: Option<ManifestRefresh>,
    cancelled: bool,
    negative: Option<(ManifestLookup, ManifestDiscoveryError, MonoTime)>,
    sequence: u64,
    observation_sequence: u64,
    now: MonoTime,
    closed: bool,
    failed: bool,
}
impl<S: SecureSession, C: ManifestCache> NativeRemoteManifestDiscovery<S, C> {
    pub fn new(
        session: S,
        source: PeerIdentity,
        authority: GroupIdentity,
        cache: C,
        config: RemoteManifestConfig,
        now: MonoTime,
    ) -> Result<Self, (RemoteManifestError, S, C)> {
        if let Err(e) = config.validate() {
            return Err((e, session, cache));
        }
        if cache.usage()
            != (ManifestCacheUsage {
                manifests: 0,
                bytes: 0,
            })
            || cache.limits().validate().is_err()
        {
            return Err((ManifestDiscoveryError::InvalidLimits.into(), session, cache));
        }
        let channel = match Channel::new(session, source) {
            Ok(c) => c,
            Err((e, s)) => return Err((e, s, cache)),
        };
        Ok(Self {
            channel,
            cache,
            authority,
            config,
            observations: BTreeMap::new(),
            pending: None,
            cancelled: false,
            negative: None,
            sequence: 0,
            observation_sequence: 0,
            now,
            closed: false,
            failed: false,
        })
    }
    pub fn pending(&self) -> Option<ManifestRefresh> {
        self.pending
    }
    pub fn source_failed(&self) -> bool {
        self.failed
    }
    pub fn usage(&self) -> ManifestCacheUsage {
        self.cache.usage()
    }
    pub fn cancel(&mut self, request: ManifestRefresh) -> bool {
        if self.pending != Some(request) {
            return false;
        }
        self.cancelled = true;
        true
    }
    pub fn into_parts(self) -> Result<(S, C), Box<Self>> {
        if (self.closed || self.failed) && self.pending.is_none() {
            Ok((self.channel.into_session(), self.cache))
        } else {
            Err(Box::new(self))
        }
    }
    pub fn replace_session(&mut self, session: S) -> Result<S, (RemoteManifestError, S)> {
        if self.closed || !self.failed || self.pending.is_some() {
            return Err((ManifestDiscoveryError::Unavailable.into(), session));
        }
        let old = self.channel.binding;
        let source = PeerIdentity {
            node: old.peer.node,
            store: old.peer.store.identity,
        };
        let channel = Channel::new(session, source)?;
        if channel.binding.local != old.local || channel.binding.generation <= old.generation {
            return Err((
                ManifestDiscoveryError::WrongIdentity.into(),
                channel.into_session(),
            ));
        }
        let old = std::mem::replace(&mut self.channel, channel);
        self.failed = false;
        self.negative = None;
        Ok(old.into_session())
    }
    fn time(&mut self, now: MonoTime) -> Result<(), RemoteManifestError> {
        if now < self.now {
            return Err(ManifestDiscoveryError::TimeWentBack.into());
        }
        self.now = now;
        Ok(())
    }
    fn fail(&mut self) {
        self.failed = true;
        self.channel.close();
    }
    fn finish(
        &mut self,
        result: Result<ManifestObservationId, RemoteManifestError>,
    ) -> Option<ManifestRefreshCompletion> {
        let request = self.pending.take()?;
        self.cancelled = false;
        if let Err(error) = result {
            let error = match error {
                RemoteManifestError::Discovery(e) => e,
                _ => ManifestDiscoveryError::Unavailable,
            };
            self.negative = Some((
                request.query,
                error,
                MonoTime(self.now.0.saturating_add(self.config.retry_ms)),
            ));
        } else {
            self.negative = None;
        }
        Some(ManifestRefreshCompletion { request, result })
    }
    pub fn poll(
        &mut self,
        now: MonoTime,
        budget: SessionPollBudget,
    ) -> Result<Option<ManifestRefreshCompletion>, RemoteManifestError> {
        budget.validate()?;
        self.time(now)?;
        if self.closed {
            return Ok(self.finish(Err(ManifestDiscoveryError::Closed.into())));
        }
        if self.failed {
            return Ok(None);
        }
        if self.pending.is_some_and(|r| now >= r.deadline) {
            self.fail();
            return Ok(self.finish(Err(RemoteManifestError::Timeout)));
        }
        let frame = match self.channel.poll(now, budget) {
            Ok(f) => f,
            Err(e) => {
                self.fail();
                return self.failure(e);
            }
        };
        let Some(frame) = frame else {
            return Ok(None);
        };
        let Some(request) = self.pending else {
            self.fail();
            return Err(RemoteManifestError::Protocol);
        };
        if frame.sequence != request.sequence
            || frame.query != request.query
            || matches!(frame.payload, Payload::Request)
            || self.channel.pending_output()
        {
            self.fail();
            return self.failure(RemoteManifestError::Protocol);
        }
        let result = if self.cancelled {
            Err(RemoteManifestError::Cancelled)
        } else {
            self.observe(request, frame.payload, now)
        };
        if result == Err(RemoteManifestError::Protocol) {
            self.fail();
        }
        Ok(self.finish(result))
    }
    fn failure(
        &mut self,
        error: RemoteManifestError,
    ) -> Result<Option<ManifestRefreshCompletion>, RemoteManifestError> {
        self.finish(Err(error)).map(Some).ok_or(error)
    }
    fn observe(
        &mut self,
        request: ManifestRefresh,
        payload: Payload,
        now: MonoTime,
    ) -> Result<ManifestObservationId, RemoteManifestError> {
        let (manifest, lifetime_ms) = match payload {
            Payload::Hint {
                manifest,
                lifetime_ms,
            } => (manifest, lifetime_ms),
            Payload::Missing => {
                if let Some(old) = self
                    .observations
                    .get_mut(&request.query.locator.responsibility)
                {
                    old.live = false;
                }
                return Err(ManifestDiscoveryError::Missing.into());
            }
            Payload::Unavailable => return Err(ManifestDiscoveryError::Unavailable.into()),
            Payload::Request => return Err(RemoteManifestError::Protocol),
        };
        request.query.check(&manifest)?;
        if lifetime_ms == 0 || lifetime_ms > self.config.max_lifetime_ms {
            return Err(RemoteManifestError::Protocol);
        }
        // Lifetime starts at request admission: network delay cannot extend a
        // remote cache lease or require synchronized clocks.
        let expires = MonoTime(
            request
                .started_at
                .0
                .checked_add(lifetime_ms)
                .ok_or(ManifestDiscoveryError::Exhausted)?,
        );
        if expires <= now {
            return Err(ManifestDiscoveryError::Expired.into());
        }
        let next = self
            .observation_sequence
            .checked_add(1)
            .ok_or(ManifestDiscoveryError::Exhausted)?;
        let id = ManifestObservationId::new(next).unwrap();
        if !self
            .observations
            .contains_key(&request.query.locator.responsibility)
            && self.observations.len() >= self.cache.limits().manifests
        {
            return Err(ManifestDiscoveryError::Overloaded.into());
        }
        self.cache
            .admit(*manifest)
            .map_err(|(e, _)| ManifestDiscoveryError::Routing(e))?;
        self.observation_sequence = next;
        self.observations.insert(
            request.query.locator.responsibility,
            Observation {
                id,
                expires,
                live: true,
            },
        );
        Ok(id)
    }
    fn cached(
        &self,
        request: ManifestLookup,
        now: MonoTime,
    ) -> Result<ManifestObservation, ManifestDiscoveryError> {
        let observed = self
            .observations
            .get(&request.locator.responsibility)
            .ok_or(ManifestDiscoveryError::Missing)?;
        if !observed.live || now >= observed.expires {
            return Err(ManifestDiscoveryError::Expired);
        }
        let manifest = self
            .cache
            .get(request.locator.responsibility)
            .ok_or(ManifestDiscoveryError::ProviderViolation)?;
        request.check(manifest)?;
        Ok(ManifestObservation {
            locator: request.locator,
            observation: observed.id,
            manifest: manifest.clone(),
            expires_at: observed.expires,
        })
    }
}
impl<S: SecureSession, C: ManifestCache> ManifestDiscovery for NativeRemoteManifestDiscovery<S, C> {
    fn lookup(
        &mut self,
        request: ManifestLookup,
        now: MonoTime,
    ) -> Result<ManifestObservation, ManifestDiscoveryError> {
        if self.closed {
            return Err(ManifestDiscoveryError::Closed);
        }
        self.time(now).map_err(|e| match e {
            RemoteManifestError::Discovery(e) => e,
            _ => ManifestDiscoveryError::Unavailable,
        })?;
        if request.locator.authority != self.authority {
            return Err(ManifestDiscoveryError::WrongAuthority);
        }
        match self.cached(request, now) {
            Ok(observed) => return Ok(observed),
            Err(
                ManifestDiscoveryError::Missing
                | ManifestDiscoveryError::Expired
                | ManifestDiscoveryError::Routing(
                    RoutingError::EpochRegression | RoutingError::StaleGeneration,
                ),
            ) => (),
            Err(e) => return Err(e),
        }
        if let Some((old, error, until)) = self.negative {
            if old == request && now < until {
                return Err(error);
            }
        }
        if let Some(pending) = self.pending {
            return Err(if pending.query == request {
                ManifestDiscoveryError::Unavailable
            } else {
                ManifestDiscoveryError::Overloaded
            });
        }
        if self.failed {
            return Err(ManifestDiscoveryError::Unavailable);
        }
        let sequence = self
            .sequence
            .checked_add(1)
            .ok_or(ManifestDiscoveryError::Exhausted)?;
        let deadline = MonoTime(
            now.0
                .checked_add(self.config.timeout_ms)
                .ok_or(ManifestDiscoveryError::Exhausted)?,
        );
        self.channel
            .queue(Frame {
                sequence,
                query: request,
                payload: Payload::Request,
            })
            .map_err(|_| ManifestDiscoveryError::ProviderViolation)?;
        self.sequence = sequence;
        self.cancelled = false;
        self.pending = Some(ManifestRefresh {
            binding: self.channel.binding,
            sequence,
            query: request,
            started_at: now,
            deadline,
        });
        Err(ManifestDiscoveryError::Unavailable)
    }
    fn invalidate(&mut self, locator: AuthorityLocator, observed: ManifestObservationId) -> bool {
        if self.closed || locator.authority != self.authority {
            return false;
        }
        let Some(old) = self.observations.get_mut(&locator.responsibility) else {
            return false;
        };
        if old.id != observed || !old.live {
            return false;
        }
        old.live = false;
        true
    }
    fn close(&mut self) {
        self.closed = true;
        self.channel.close();
    }
}
