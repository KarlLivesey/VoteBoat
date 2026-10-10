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
//! Explicitly polled composition of authenticated per-authority sources.
use super::{
    authority_endpoints::Authorities,
    command_client::{self, Attempt},
    command_endpoints::Endpoint,
    directory_connection::ACK,
    service_access::{Channel, ClientAccess},
    setup::{checked, Failure},
};
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};
use voteboat::{
    identity::*,
    native::{remote_manifest::*, routing::NativeManifestCache},
    routing::*,
    runtime::MonoTime,
    secure::{SecureSession, SessionPollBudget},
};
type Remote = NativeRemoteManifestDiscovery<Box<dyn SecureSession>, NativeManifestCache>;
struct Active {
    authority: GroupIdentity,
    remote: Remote,
    expires: MonoTime,
    generation: u64,
}
struct Observation {
    external: ManifestObservationId,
    local: ManifestObservationId,
    generation: u64,
}
pub struct Discovery {
    authorities: Authorities,
    access: ClientAccess,
    start: Instant,
    deadline: Instant,
    request: Option<ManifestLookup>,
    active: Option<Active>,
    cursor: usize,
    attempts: usize,
    next_attempt: MonoTime,
    generation: u64,
    observations: BTreeMap<(GroupIdentity, ResponsibilityIdentity), Observation>,
    next_observation: u64,
    closed: bool,
}
impl Discovery {
    pub fn new(authorities: Authorities, access: ClientAccess, start: Instant) -> Self {
        Self {
            authorities,
            access,
            start,
            deadline: start + Duration::from_secs(10),
            request: None,
            active: None,
            cursor: 0,
            attempts: 0,
            next_attempt: MonoTime(0),
            generation: 0,
            observations: BTreeMap::new(),
            next_observation: 1,
            closed: false,
        }
    }
    pub fn now(&self) -> MonoTime {
        MonoTime(self.start.elapsed().as_millis().min(u64::MAX as u128) as u64)
    }
    fn clear_active(&mut self) {
        if let Some(mut active) = self.active.take() {
            active.remote.close();
            let _ = active.remote.poll(self.now(), SessionPollBudget::default());
        }
    }
    pub fn poll(&mut self) -> Result<(), Failure> {
        if self.closed {
            return Err("recursive discovery is closed".into());
        }
        let now = self.now();
        if Instant::now() >= self.deadline {
            return Err("recursive lookup deadline expired".into());
        }
        let Some(request) = self.request else {
            return Ok(());
        };
        if self
            .active
            .as_ref()
            .is_some_and(|a| a.authority != request.locator.authority)
        {
            self.clear_active();
        }
        if self.active.is_some() {
            return self.poll_active(now);
        }
        if now < self.next_attempt {
            return Ok(());
        }
        if self.attempts == 128 {
            return Err("recursive lookup connection budget exhausted".into());
        }
        let peers = self
            .authorities
            .groups
            .get(&request.locator.authority)
            .ok_or("authority not provisioned")?;
        let endpoint = peers[self.cursor % peers.len()].clone();
        self.cursor += 1;
        self.attempts += 1;
        self.next_attempt = MonoTime(now.0 + 25);
        if let Some(remote) = self.connect(&endpoint, request.locator.authority)? {
            self.generation = self
                .generation
                .checked_add(1)
                .ok_or("source generation exhausted")?;
            self.active = Some(Active {
                authority: request.locator.authority,
                remote,
                expires: MonoTime(now.0 + 2000),
                generation: self.generation,
            });
        }
        Ok(())
    }
    fn poll_active(&mut self, now: MonoTime) -> Result<(), Failure> {
        let active = self.active.as_mut().unwrap();
        let completion = active
            .remote
            .poll(now, SessionPollBudget::default())
            .map_err(|e| format!("manifest transport: {e:?}"))?;
        if let Some(completion) = completion {
            match completion.result {
                Ok(_) => (),
                Err(RemoteManifestError::Discovery(ManifestDiscoveryError::Unavailable)) => {
                    self.clear_active();
                    return Ok(());
                }
                Err(e) => return Err(format!("manifest response: {e:?}").into()),
            }
        }
        if self
            .active
            .as_ref()
            .is_some_and(|a| a.remote.pending().is_some() && now >= a.expires)
        {
            self.clear_active();
        }
        Ok(())
    }
    fn connect(
        &self,
        endpoint: &Endpoint,
        authority: GroupIdentity,
    ) -> Result<Option<Remote>, Failure> {
        let deadline = self
            .deadline
            .min(Instant::now() + Duration::from_millis(1500));
        let Some(mut probe) = self.channel(endpoint, deadline)? else {
            return Ok(None);
        };
        let reply = command_client::request(&mut probe, b"status\n", deadline, self.start);
        let leader = match reply {
            Attempt::Reply(text) if text.starts_with("OK ") => {
                text.split_whitespace().any(|w| w == "role=Leader")
            }
            Attempt::Reply(text) => {
                return Err(format!("authority status refused: {}", text.trim()).into())
            }
            Attempt::Interrupted(reason) => {
                return Err(format!("authority status interrupted: {reason}").into())
            }
            Attempt::Unavailable => return Ok(None),
        };
        drop(probe);
        if !leader {
            return Ok(None);
        }
        let Some(mut channel) = self.channel(endpoint, deadline)? else {
            return Ok(None);
        };
        match command_client::request(&mut channel, b"manifest-session\n", deadline, self.start) {
            Attempt::Reply(reply) if reply == ACK => (),
            _ => return Err("authority manifest upgrade refused or interrupted".into()),
        }
        let cache = checked(NativeManifestCache::new(ManifestCacheLimits {
            manifests: 64,
            bytes: 256 * 1024,
        }))?;
        let remote = NativeRemoteManifestDiscovery::new(
            channel.take_secure().ok_or("missing secure session")?,
            super::service_access::transport_identity(endpoint.node, false),
            authority,
            cache,
            RemoteManifestConfig::default(),
            self.now(),
        )
        .map_err(|(e, _, _)| format!("manifest source setup: {e:?}"))?;
        Ok(Some(remote))
    }
    fn channel(&self, endpoint: &Endpoint, deadline: Instant) -> Result<Option<Channel>, Failure> {
        match command_client::connect(endpoint, deadline, Some(&self.access), self.start) {
            Ok(c) => Ok(Some(c)),
            Err(Attempt::Unavailable) => Ok(None),
            Err(Attempt::Interrupted(reason)) => {
                Err(format!("authority authentication/connection failed: {reason}").into())
            }
            Err(Attempt::Reply(_)) => Err("unexpected connection reply".into()),
        }
    }
    fn observation(
        &mut self,
        key: (GroupIdentity, ResponsibilityIdentity),
        local: ManifestObservationId,
        generation: u64,
    ) -> Result<ManifestObservationId, ManifestDiscoveryError> {
        if let Some(old) = self.observations.get(&key) {
            if old.local == local && old.generation == generation {
                return Ok(old.external);
            }
        }
        if !self.observations.contains_key(&key) && self.observations.len() == 64 {
            return Err(ManifestDiscoveryError::Overloaded);
        }
        let external = ManifestObservationId::new(self.next_observation)
            .ok_or(ManifestDiscoveryError::Exhausted)?;
        self.next_observation = self
            .next_observation
            .checked_add(1)
            .ok_or(ManifestDiscoveryError::Exhausted)?;
        self.observations.insert(
            key,
            Observation {
                external,
                local,
                generation,
            },
        );
        Ok(external)
    }
}
impl ManifestDiscovery for Discovery {
    fn lookup(
        &mut self,
        request: ManifestLookup,
        now: MonoTime,
    ) -> Result<ManifestObservation, ManifestDiscoveryError> {
        if self.closed {
            return Err(ManifestDiscoveryError::Closed);
        }
        if !self
            .authorities
            .groups
            .contains_key(&request.locator.authority)
        {
            return Err(ManifestDiscoveryError::WrongAuthority);
        }
        if self.request != Some(request) {
            if self
                .active
                .as_ref()
                .is_some_and(|a| a.remote.pending().is_some())
            {
                return Err(ManifestDiscoveryError::Overloaded);
            }
            self.request = Some(request);
            self.cursor = 0;
        }
        let Some(active) = self
            .active
            .as_mut()
            .filter(|a| a.authority == request.locator.authority)
        else {
            return Err(ManifestDiscoveryError::Unavailable);
        };
        let pending = active.remote.pending().is_some();
        let result = active.remote.lookup(request, now);
        if !pending && active.remote.pending().is_some() {
            active.expires = MonoTime(
                now.0
                    .checked_add(2000)
                    .ok_or(ManifestDiscoveryError::Exhausted)?,
            );
        }
        let generation = active.generation;
        let mut observation = result?;
        observation.observation = self.observation(
            (request.locator.authority, request.locator.responsibility),
            observation.observation,
            generation,
        )?;
        Ok(observation)
    }
    fn invalidate(&mut self, locator: AuthorityLocator, observed: ManifestObservationId) -> bool {
        let key = (locator.authority, locator.responsibility);
        let (Some(active), Some(old)) = (self.active.as_mut(), self.observations.get(&key)) else {
            return false;
        };
        if old.external != observed
            || old.generation != active.generation
            || active.authority != locator.authority
        {
            return false;
        }
        active.remote.invalidate(locator, old.local)
    }
    fn close(&mut self) {
        self.closed = true;
        self.clear_active();
    }
}
impl Drop for Discovery {
    fn drop(&mut self) {
        self.clear_active();
    }
}
