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
//! Bounded discovery of missing manifest segments; cached routes remain hints.
use super::*;
use crate::runtime::MonoTime;
use std::num::NonZeroU64;
pub const MANIFEST_DISCOVERY_CONTRACT_VERSION: u32 = 1;
pub const MANIFEST_READ_SOURCE_CONTRACT_VERSION: u32 = 2;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuthorityLocator {
    pub responsibility: ResponsibilityIdentity,
    pub authority: GroupIdentity,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ManifestObservationId(NonZeroU64);
impl ManifestObservationId {
    pub fn new(value: u64) -> Option<Self> {
        NonZeroU64::new(value).map(Self)
    }
    pub fn get(self) -> u64 {
        self.0.get()
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ManifestLookup {
    pub locator: AuthorityLocator,
    pub minimum_epoch: Option<OwnershipEpoch>,
    pub minimum_generation: Option<RouteGeneration>,
}
#[derive(Debug)]
pub struct ManifestObservation {
    pub locator: AuthorityLocator,
    pub observation: ManifestObservationId,
    pub manifest: ResponsibilityManifest,
    pub expires_at: MonoTime,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManifestDiscoveryError {
    InvalidLimits,
    WrongAuthority,
    WrongIdentity,
    Expired,
    Closed,
    Unavailable,
    Overloaded,
    Cancelled,
    ProviderViolation,
    Missing,
    StaleObservation,
    TimeWentBack,
    Exhausted,
    BudgetExhausted(ResponsibilityIdentity),
    Routing(RoutingError),
}
impl ManifestLookup {
    pub fn check(&self, manifest: &ResponsibilityManifest) -> Result<(), ManifestDiscoveryError> {
        let input = manifest.input();
        if input.responsibility != self.locator.responsibility {
            return Err(ManifestDiscoveryError::WrongIdentity);
        }
        if input.authority != self.locator.authority {
            return Err(ManifestDiscoveryError::WrongAuthority);
        }
        if self.minimum_epoch.is_some_and(|e| input.epoch < e) {
            return Err(ManifestDiscoveryError::Routing(
                RoutingError::EpochRegression,
            ));
        }
        if self
            .minimum_generation
            .is_some_and(|g| input.generation < g)
        {
            return Err(ManifestDiscoveryError::Routing(
                RoutingError::StaleGeneration,
            ));
        }
        if manifest.retained_bytes() > MAX_MANIFEST_BYTES {
            return Err(ManifestDiscoveryError::Routing(RoutingError::Capacity));
        }
        Ok(())
    }
}
/// Bounded nonblocking lookup of a previously authorized directory observation.
/// Hosts drive quorum reads/remote refresh separately. Returned owned manifests
/// and clones must fit MAX_MANIFEST_BYTES. Invalidation is observation-specific,
/// never a membership/ownership change. Close affects the selected view only.
pub trait ManifestDiscovery {
    fn lookup(
        &mut self,
        request: ManifestLookup,
        now: MonoTime,
    ) -> Result<ManifestObservation, ManifestDiscoveryError>;
    fn invalidate(&mut self, locator: AuthorityLocator, observed: ManifestObservationId) -> bool;
    fn close(&mut self);
}
/// Original trusted application reads, not serialized assertions of read authority.
/// Calls are bounded/nonblocking. Binding stays immutable. Submission refusal
/// accepts nothing; accepted reads retain their original Node credits until the
/// exact completion is consumed. Cancellation ends observation, not consensus.
/// This source's read-result consumer is exclusive while a lookup is pending.
pub trait ManifestReadSource {
    /// Original application result retained on rejected completion; no erasure.
    type ReadResult;
    fn binding(&self) -> crate::runtime::ReadInvocationBinding;
    fn pending_reads(&self) -> usize;
    fn submit(
        &mut self,
        request: ManifestLookup,
    ) -> Result<
        crate::runtime::ReadInvocationTicket,
        crate::runtime::ReadInvocationRejected<ResponsibilityIdentity>,
    >;
    fn poll_result(
        &mut self,
        ticket: crate::runtime::ReadInvocationTicket,
    ) -> Result<
        Option<crate::runtime::ReadOutcome<Option<ResponsibilityManifest>>>,
        crate::runtime::ReadCompletionRejected<Self::ReadResult>,
    >;
    fn cancel(
        &mut self,
        ticket: crate::runtime::ReadInvocationTicket,
    ) -> Result<(), crate::runtime::ReadInvocationError>;
}
#[derive(Clone, Copy, Debug)]
pub struct DiscoverRouteRequest<'a> {
    pub start: ManifestLookup,
    pub key: &'a [u8],
    pub max_hops: usize,
    pub lookups: usize,
    pub now: MonoTime,
}
fn missing_lookup<C: ManifestCache, P: PartitionPolicy>(
    cache: &C,
    policy: &P,
    r: DiscoverRouteRequest<'_>,
    missing: ResponsibilityIdentity,
) -> Result<ManifestLookup, ManifestDiscoveryError> {
    let mut query = r.start;
    let bucket = super::bucket(policy, r.key).map_err(ManifestDiscoveryError::Routing)?;
    for _ in 0..r.max_hops {
        if query.locator.responsibility == missing {
            return Ok(query);
        }
        let manifest = cache
            .get(query.locator.responsibility)
            .ok_or(ManifestDiscoveryError::Missing)?;
        query.check(manifest)?;
        match manifest
            .select(bucket)
            .map_err(ManifestDiscoveryError::Routing)?
            .target
        {
            RouteTarget::Child(child) => {
                query = ManifestLookup {
                    locator: AuthorityLocator {
                        responsibility: child.responsibility,
                        authority: child.group,
                    },
                    minimum_epoch: Some(child.epoch),
                    minimum_generation: None,
                }
            }
            RouteTarget::Group(_) | RouteTarget::Vacant => {
                return Err(ManifestDiscoveryError::WrongIdentity)
            }
        }
    }
    Err(ManifestDiscoveryError::Routing(RoutingError::HopLimit))
}
/// Fill only missing path segments, then reuse the ordinary checked resolver.
/// Existing cached child routes never require a live ancestor/source lookup.
pub fn resolve_discovered<C: ManifestCache, P: PartitionPolicy, D: ManifestDiscovery>(
    cache: &mut C,
    policy: &P,
    discovery: &mut D,
    request: DiscoverRouteRequest<'_>,
) -> Result<RouteHint, ManifestDiscoveryError> {
    if request.max_hops == 0
        || request.max_hops > MAX_ROUTE_HOPS
        || request.lookups > request.max_hops
    {
        return Err(ManifestDiscoveryError::InvalidLimits);
    }
    let mut remaining = request.lookups;
    loop {
        if let Some(start) = cache.get(request.start.locator.responsibility) {
            request.start.check(start)?;
        }
        match resolve(
            cache,
            policy,
            request.start.locator.responsibility,
            request.key,
            request.max_hops,
        ) {
            Ok(hint) => return Ok(hint),
            Err(RoutingError::Missing(missing)) => {
                if remaining == 0 {
                    return Err(ManifestDiscoveryError::BudgetExhausted(missing));
                }
                let query = missing_lookup(cache, policy, request, missing)?;
                let observation = discovery.lookup(query, request.now)?;
                if observation.locator != query.locator {
                    return Err(ManifestDiscoveryError::WrongAuthority);
                }
                if request.now >= observation.expires_at {
                    return Err(ManifestDiscoveryError::Expired);
                }
                query.check(&observation.manifest)?;
                cache
                    .admit(observation.manifest)
                    .map_err(|(e, _)| ManifestDiscoveryError::Routing(e))?;
                remaining -= 1;
            }
            Err(error) => return Err(ManifestDiscoveryError::Routing(error)),
        }
    }
}
