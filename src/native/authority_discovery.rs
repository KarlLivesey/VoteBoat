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
//! Explicit observations from completed quorum-backed Directory reads.
use crate::{
    identity::*,
    native::routing::NativeManifestCache,
    raft::ReadBarrier,
    routing::*,
    runtime::{MonoTime, ReadInvocationBinding, ReadInvocationTicket, ReadOutcome},
};
use std::collections::BTreeMap;
struct Observation {
    id: ManifestObservationId,
    sequence: u64,
    source_generation: u64,
    barrier: ReadBarrier,
    expires_at: MonoTime,
    live: bool,
}
pub struct NativeAuthorityDiscovery {
    cache: NativeManifestCache,
    observations: BTreeMap<ResponsibilityIdentity, Observation>,
    lifetime_ms: u64,
    binding: ReadInvocationBinding,
    highest_sequence: u64,
    source_generation: u64,
    next: u64,
    now: MonoTime,
    closed: bool,
}
impl NativeAuthorityDiscovery {
    pub fn new(
        limits: ManifestCacheLimits,
        lifetime_ms: u64,
        binding: ReadInvocationBinding,
        now: MonoTime,
    ) -> Result<Self, ManifestDiscoveryError> {
        if lifetime_ms == 0 || lifetime_ms > 3_600_000 {
            return Err(ManifestDiscoveryError::InvalidLimits);
        }
        Ok(Self {
            cache: NativeManifestCache::new(limits).map_err(ManifestDiscoveryError::Routing)?,
            observations: BTreeMap::new(),
            lifetime_ms,
            binding,
            highest_sequence: 0,
            source_generation: 0,
            next: 0,
            now,
            closed: false,
        })
    }
    /// Change the read-ticket domain without forgetting manifest/barrier floors.
    /// Generations and observation IDs are volatile and end with this provider.
    pub(crate) fn replace_binding(
        &mut self,
        binding: ReadInvocationBinding,
        now: MonoTime,
    ) -> Result<(), ManifestDiscoveryError> {
        if self.closed {
            return Err(ManifestDiscoveryError::Closed);
        }
        if now < self.now {
            return Err(ManifestDiscoveryError::TimeWentBack);
        }
        if binding == self.binding {
            return Err(ManifestDiscoveryError::WrongAuthority);
        }
        let generation = self
            .source_generation
            .checked_add(1)
            .ok_or(ManifestDiscoveryError::Exhausted)?;
        for observation in self.observations.values_mut() {
            observation.live = false;
        }
        self.binding = binding;
        self.source_generation = generation;
        self.highest_sequence = 0;
        self.now = now;
        Ok(())
    }
    fn check_observation(
        &mut self,
        request: &ManifestLookup,
        ticket: &ReadInvocationTicket,
        outcome: &ReadOutcome<Option<ResponsibilityManifest>>,
        now: MonoTime,
    ) -> Result<Option<ManifestObservationId>, ManifestDiscoveryError> {
        if self.closed {
            return Err(ManifestDiscoveryError::Closed);
        }
        if now < self.now {
            return Err(ManifestDiscoveryError::TimeWentBack);
        }
        if ticket.binding != self.binding
            || ticket.group != request.locator.authority
            || ticket.sequence == 0
        {
            return Err(ManifestDiscoveryError::WrongAuthority);
        }
        let (barrier, manifest) = match outcome {
            ReadOutcome::Read {
                barrier,
                result: Ok(Some(manifest)),
            } => (barrier, manifest),
            ReadOutcome::Read {
                barrier,
                result: Ok(None),
            } => {
                if barrier.group() != request.locator.authority
                    || barrier.request() != ticket.request
                {
                    return Err(ManifestDiscoveryError::WrongAuthority);
                }
                if self
                    .cache
                    .get(request.locator.responsibility)
                    .is_some_and(|m| m.input().authority != request.locator.authority)
                {
                    return Err(ManifestDiscoveryError::WrongAuthority);
                }
                if ticket.sequence <= self.highest_sequence {
                    return Err(ManifestDiscoveryError::StaleObservation);
                }
                if let Some(prior) = self.observations.get_mut(&request.locator.responsibility) {
                    if barrier.index() < prior.barrier.index()
                        || barrier.term() < prior.barrier.term()
                    {
                        return Err(ManifestDiscoveryError::StaleObservation);
                    }
                    prior.barrier = *barrier;
                    prior.sequence = ticket.sequence;
                    prior.source_generation = self.source_generation;
                    prior.live = false;
                }
                self.highest_sequence = ticket.sequence;
                self.now = now;
                return Err(ManifestDiscoveryError::Missing);
            }
            _ => return Err(ManifestDiscoveryError::Unavailable),
        };
        if barrier.group() != request.locator.authority || barrier.request() != ticket.request {
            return Err(ManifestDiscoveryError::WrongAuthority);
        }
        request.check(manifest)?;
        if let Some(prior) = self.observations.get(&request.locator.responsibility) {
            if barrier.index() < prior.barrier.index() || barrier.term() < prior.barrier.term() {
                return Err(ManifestDiscoveryError::StaleObservation);
            }
            if prior.source_generation == self.source_generation
                && *barrier == prior.barrier
                && ticket.sequence == prior.sequence
            {
                if !prior.live
                    || now >= prior.expires_at
                    || self.cache.get(request.locator.responsibility) != Some(manifest)
                {
                    return Err(ManifestDiscoveryError::StaleObservation);
                }
                return Ok(Some(prior.id));
            }
        }
        if ticket.sequence <= self.highest_sequence {
            return Err(ManifestDiscoveryError::StaleObservation);
        }
        now.0
            .checked_add(self.lifetime_ms)
            .ok_or(ManifestDiscoveryError::Exhausted)?;
        self.next
            .checked_add(1)
            .ok_or(ManifestDiscoveryError::Exhausted)?;
        Ok(None)
    }
    pub fn usage(&self) -> ManifestCacheUsage {
        self.cache.usage()
    }
    /// The trusted host supplies the completed original Directory read, not a
    /// replayed network object. A ReadOutcome is not an external signed credential.
    #[allow(clippy::result_large_err)]
    pub fn observe(
        &mut self,
        request: ManifestLookup,
        ticket: ReadInvocationTicket,
        outcome: ReadOutcome<Option<ResponsibilityManifest>>,
        now: MonoTime,
    ) -> Result<
        ManifestObservationId,
        (
            ManifestDiscoveryError,
            ReadOutcome<Option<ResponsibilityManifest>>,
        ),
    > {
        let check = self.check_observation(&request, &ticket, &outcome, now);
        match check {
            Err(error) => return Err((error, outcome)),
            Ok(Some(id)) => {
                self.now = now;
                return Ok(id);
            }
            Ok(None) => (),
        }
        let ReadOutcome::Read {
            barrier,
            result: Ok(Some(manifest)),
        } = outcome
        else {
            unreachable!()
        };
        if let Err((error, manifest)) = self.cache.admit(manifest) {
            return Err((
                ManifestDiscoveryError::Routing(error),
                ReadOutcome::Read {
                    barrier,
                    result: Ok(Some(manifest)),
                },
            ));
        }
        self.next += 1;
        let id = ManifestObservationId::new(self.next).unwrap();
        self.observations.insert(
            request.locator.responsibility,
            Observation {
                id,
                sequence: ticket.sequence,
                source_generation: self.source_generation,
                barrier,
                expires_at: MonoTime(now.0 + self.lifetime_ms),
                live: true,
            },
        );
        self.highest_sequence = ticket.sequence;
        self.now = now;
        Ok(id)
    }
}
impl ManifestDiscovery for NativeAuthorityDiscovery {
    fn lookup(
        &mut self,
        request: ManifestLookup,
        now: MonoTime,
    ) -> Result<ManifestObservation, ManifestDiscoveryError> {
        if self.closed {
            return Err(ManifestDiscoveryError::Closed);
        }
        if now < self.now {
            return Err(ManifestDiscoveryError::TimeWentBack);
        }
        self.now = now;
        let prior = self
            .observations
            .get(&request.locator.responsibility)
            .ok_or(ManifestDiscoveryError::Missing)?;
        if !prior.live {
            return Err(ManifestDiscoveryError::Missing);
        }
        if now >= prior.expires_at {
            return Err(ManifestDiscoveryError::Expired);
        }
        let manifest = self
            .cache
            .get(request.locator.responsibility)
            .ok_or(ManifestDiscoveryError::Missing)?;
        request.check(manifest)?;
        Ok(ManifestObservation {
            locator: request.locator,
            observation: prior.id,
            manifest: manifest.clone(),
            expires_at: prior.expires_at,
        })
    }
    fn invalidate(&mut self, locator: AuthorityLocator, observed: ManifestObservationId) -> bool {
        if self.closed
            || self
                .cache
                .get(locator.responsibility)
                .is_none_or(|m| m.input().authority != locator.authority)
        {
            return false;
        }
        let Some(prior) = self.observations.get_mut(&locator.responsibility) else {
            return false;
        };
        if prior.id != observed || !prior.live {
            return false;
        }
        prior.live = false;
        true
    }
    fn close(&mut self) {
        self.closed = true;
    }
}
