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
//! Checked responsibility manifests and bounded routing hints, without I/O.
//!
//! Cache contents never authorize execution. Callers obtain manifests from an
//! authorized committed/applied directory. Servers independently check their
//! local committed ownership at both admission and application. No route edit
//! implements fencing, import or activation of a transferred scope.
use crate::{identity::*, placement::PlacementRequirements};
use std::mem::size_of;
pub(crate) mod codec;

pub const ROUTING_CONTRACT_VERSION: u32 = 1;
pub const MAX_MANIFEST_ROUTES: usize = 256;
pub const MAX_MANIFEST_BYTES: usize = 32768;
pub const MAX_ROUTE_HOPS: usize = 32;
pub const MAX_ROUTING_KEY_BYTES: usize = 4096;
pub const MAX_CACHE_MANIFESTS: usize = 4096;
pub const MAX_CACHE_BYTES: usize = 64 * 1024 * 1024;

/// Stable logical buckets, independent of physical group or lane counts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BucketRange {
    start: u16,
    end: u16,
}
impl BucketRange {
    /// Half-open [start, end) in the fixed 256-bucket universe.
    pub fn new(start: u16, end: u16) -> Result<Self, RoutingError> {
        if start >= end || end > 256 {
            return Err(RoutingError::InvalidRange);
        }
        Ok(Self { start, end })
    }
    pub fn start(self) -> u16 {
        self.start
    }
    pub fn end(self) -> u16 {
        self.end
    }
    pub fn contains(self, bucket: u16) -> bool {
        self.start <= bucket && bucket < self.end
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PartitionScheme {
    pub id: RoutingSchemeId,
    pub version: u32,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApplicationAdapter {
    pub id: ApplicationAdapterId,
    pub version: u32,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParentAuthority {
    pub responsibility: ResponsibilityIdentity,
    pub group: GroupIdentity,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChildAuthority {
    pub responsibility: ResponsibilityIdentity,
    pub group: GroupIdentity,
    pub epoch: OwnershipEpoch,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteTarget {
    Group(GroupIdentity),
    Child(ChildAuthority),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RouteEntry {
    pub scope: BucketRange,
    pub target: RouteTarget,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutionMode {
    /// All operations in this responsibility use one group's ordered prefix.
    Single(GroupIdentity),
    /// Complete, sorted coverage by concrete groups. Ordering is per group.
    Partitioned(Vec<RouteEntry>),
    /// Complete, sorted coverage by children and optionally local group scopes.
    Delegated(Vec<RouteEntry>),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResponsibilityState {
    Active,
    Fenced,
}

/// Owned construction input; rejection returns this exact value, including
/// retained vector capacity. Identity doubles as the application namespace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestInput {
    pub responsibility: ResponsibilityIdentity,
    pub parent: Option<ParentAuthority>,
    pub authority: GroupIdentity,
    pub application: ApplicationAdapter,
    pub scheme: PartitionScheme,
    pub scope: BucketRange,
    pub epoch: OwnershipEpoch,
    pub generation: RouteGeneration,
    pub placement: PlacementRequirements,
    pub state: ResponsibilityState,
    pub execution: ExecutionMode,
}
/// Immutable checked shape, not a certificate of commitment or authorization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResponsibilityManifest(ManifestInput);
impl ResponsibilityManifest {
    // Return the original owned input without allocating a rejection wrapper.
    #[allow(clippy::result_large_err)]
    pub fn new(input: ManifestInput) -> Result<Self, (RoutingError, ManifestInput)> {
        if let Err(error) = Self::validate(&input) {
            return Err((error, input));
        }
        Ok(Self(input))
    }
    fn validate(input: &ManifestInput) -> Result<(), RoutingError> {
        if input.scheme.version == 0 || input.application.version == 0 {
            return Err(RoutingError::UnsupportedSchema);
        }
        if input.placement.minimum_voting_domains == 0
            || input.placement.minimum_voting_domains > 64
        {
            return Err(RoutingError::InvalidPlacement);
        }
        if input
            .parent
            .is_some_and(|p| p.responsibility == input.responsibility)
        {
            return Err(RoutingError::Cycle);
        }
        let (entries, delegated) = match &input.execution {
            ExecutionMode::Single(_) => return Ok(()),
            ExecutionMode::Partitioned(entries) => (entries, false),
            ExecutionMode::Delegated(entries) => (entries, true),
        };
        if entries.is_empty()
            || entries.len() > MAX_MANIFEST_ROUTES
            || entries.capacity() > MAX_MANIFEST_ROUTES
        {
            return Err(RoutingError::Capacity);
        }
        let mut end = input.scope.start;
        let mut children = Vec::new();
        for entry in entries {
            if entry.scope.start != end || entry.scope.end > input.scope.end {
                return Err(RoutingError::Coverage);
            }
            end = entry.scope.end;
            if let RouteTarget::Child(child) = entry.target {
                if !delegated {
                    return Err(RoutingError::InvalidMode);
                }
                if child.responsibility == input.responsibility {
                    return Err(RoutingError::Cycle);
                }
                if children.contains(&child.responsibility) {
                    return Err(RoutingError::DuplicateChild);
                }
                children.push(child.responsibility);
            }
        }
        if end != input.scope.end {
            return Err(RoutingError::Coverage);
        }
        if delegated && children.is_empty() {
            return Err(RoutingError::InvalidMode);
        }
        Ok(())
    }
    pub fn input(&self) -> &ManifestInput {
        &self.0
    }
    pub fn into_input(self) -> ManifestInput {
        self.0
    }
    /// Charges value bytes and all retained route capacity. Collection/allocator
    /// bookkeeping is separately bounded by the fixed entry ceiling.
    pub fn retained_bytes(&self) -> usize {
        size_of::<Self>()
            + match &self.0.execution {
                ExecutionMode::Single(_) => 0,
                ExecutionMode::Partitioned(v) | ExecutionMode::Delegated(v) => {
                    v.capacity() * size_of::<RouteEntry>()
                }
            }
    }
    fn select(&self, bucket: u16) -> Result<RouteEntry, RoutingError> {
        if self.0.state != ResponsibilityState::Active {
            return Err(RoutingError::Fenced);
        }
        if !self.0.scope.contains(bucket) {
            return Err(RoutingError::OutsideScope);
        }
        match &self.0.execution {
            ExecutionMode::Single(group) => Ok(RouteEntry {
                scope: self.0.scope,
                target: RouteTarget::Group(*group),
            }),
            ExecutionMode::Partitioned(entries) | ExecutionMode::Delegated(entries) => entries
                .iter()
                .find(|r| r.scope.contains(bucket))
                .copied()
                .ok_or(RoutingError::Coverage),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RoutingError {
    InvalidRange,
    UnsupportedSchema,
    InvalidPlacement,
    InvalidMode,
    Coverage,
    DuplicateChild,
    Capacity,
    InvalidLimits,
    StaleGeneration,
    GenerationConflict,
    IdentityChange,
    EpochRegression,
    Missing(ResponsibilityIdentity),
    WrongIdentity,
    WrongParent,
    WrongChild,
    Cycle,
    HopLimit,
    InvalidKey,
    OutsideScope,
    Fenced,
    EpochMismatch,
    WrongOwner,
}

/// Deterministic bounded policy, selected by explicit persistent scheme/version.
/// No I/O, runtime hashing, live-load decisions or hidden resources. The core
/// checks the returned bucket. Hosts budget their policy's private resources.
pub trait PartitionPolicy {
    fn scheme(&self) -> PartitionScheme;
    fn bucket(&self, key: &[u8]) -> Result<u16, RoutingError>;
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ManifestCacheLimits {
    pub manifests: usize,
    pub bytes: usize,
}
impl ManifestCacheLimits {
    pub fn validate(self) -> Result<(), RoutingError> {
        if self.manifests == 0
            || self.manifests > MAX_CACHE_MANIFESTS
            || self.bytes == 0
            || self.bytes > MAX_CACHE_BYTES
        {
            return Err(RoutingError::InvalidLimits);
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ManifestCacheUsage {
    pub manifests: usize,
    pub bytes: usize,
}
/// Synchronous, caller-owned volatile hints. Admission consumes a manifest only
/// on success; all errors return the original input. No implicit eviction,
/// fetch/retry, workers, durability acknowledgement or shutdown work. Rebuild
/// from authorized directory views after restart. An empty cache is a miss,
/// never a new authority. Providers reject stale/conflicting generations and
/// immutable lineage/schema changes; invalidation names the observed generation.
/// The caller is responsible for authenticating the source before admission.
pub trait ManifestCache {
    fn get(&self, responsibility: ResponsibilityIdentity) -> Option<&ResponsibilityManifest>;
    #[allow(clippy::result_large_err)] // Preserve original owned allocation on rejection.
    fn admit(
        &mut self,
        manifest: ResponsibilityManifest,
    ) -> Result<(), (RoutingError, ResponsibilityManifest)>;
    fn invalidate(
        &mut self,
        responsibility: ResponsibilityIdentity,
        observed: RouteGeneration,
    ) -> bool;
    fn limits(&self) -> ManifestCacheLimits;
    fn usage(&self) -> ManifestCacheUsage;
}

/// A location hint, including the context to carry in a routed command. Fields
/// are public because this is not authority. Operation identity/payload and
/// authorization/leadership/dedup remain the application's existing contracts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RouteHint {
    pub responsibility: ResponsibilityIdentity,
    pub group: GroupIdentity,
    pub application: ApplicationAdapter,
    pub scheme: PartitionScheme,
    pub scope: BucketRange,
    pub bucket: u16,
    pub epoch: OwnershipEpoch,
    pub generation: RouteGeneration,
}
fn bucket<P: PartitionPolicy + ?Sized>(policy: &P, key: &[u8]) -> Result<u16, RoutingError> {
    if key.len() > MAX_ROUTING_KEY_BYTES {
        return Err(RoutingError::InvalidKey);
    }
    let bucket = policy.bucket(key)?;
    if bucket >= 256 {
        return Err(RoutingError::InvalidKey);
    }
    Ok(bucket)
}
/// Resolve only the required cached path. Starting at a previously authorized
/// child locator requires no parent access. Missing paths return one exact miss
/// for caller-controlled discovery; max_hops is 1..=32, including the start.
/// All provider results are checked for requested identity and child binding.
pub fn resolve<C: ManifestCache + ?Sized, P: PartitionPolicy + ?Sized>(
    cache: &C,
    policy: &P,
    start: ResponsibilityIdentity,
    key: &[u8],
    max_hops: usize,
) -> Result<RouteHint, RoutingError> {
    if max_hops == 0 || max_hops > MAX_ROUTE_HOPS {
        return Err(RoutingError::InvalidLimits);
    }
    let bucket = bucket(policy, key)?;
    let mut seen = [None; MAX_ROUTE_HOPS];
    let mut current = start;
    let mut expected: Option<(ParentAuthority, ChildAuthority, BucketRange)> = None;
    for hop in 0..max_hops {
        if seen[..hop].contains(&Some(current)) {
            return Err(RoutingError::Cycle);
        }
        seen[hop] = Some(current);
        let manifest = cache.get(current).ok_or(RoutingError::Missing(current))?;
        let input = manifest.input();
        if input.responsibility != current {
            return Err(RoutingError::WrongIdentity);
        }
        if input.scheme != policy.scheme() {
            return Err(RoutingError::UnsupportedSchema);
        }
        if let Some((parent, child, scope)) = expected {
            if input.parent != Some(parent) {
                return Err(RoutingError::WrongParent);
            }
            if input.authority != child.group || input.epoch != child.epoch || input.scope != scope
            {
                return Err(RoutingError::WrongChild);
            }
        }
        let route = manifest.select(bucket)?;
        match route.target {
            RouteTarget::Group(group) => {
                return Ok(RouteHint {
                    responsibility: current,
                    group,
                    application: input.application,
                    scheme: input.scheme,
                    scope: route.scope,
                    bucket,
                    epoch: input.epoch,
                    generation: input.generation,
                })
            }
            RouteTarget::Child(child) => {
                expected = Some((
                    ParentAuthority {
                        responsibility: current,
                        group: input.authority,
                    },
                    child,
                    route.scope,
                ));
                current = child.responsibility;
            }
        }
    }
    Err(RoutingError::HopLimit)
}

/// Check against LOCAL COMMITTED/APPLIED ownership, never a cache entry. Invoke
/// at admission and again in ordered apply using command-carried hint and key.
/// The selected policy must be the committed adapter's deterministic policy.
/// This does not check leadership, assignment, credentials or durable activation;
/// the embedding must establish these separately. Fenced local state refuses.
/// Route generations do not grant/revoke ownership: unchanged owner/epoch/scope
/// remains valid despite a newer location-only generation.
pub fn check_owner<P: PartitionPolicy + ?Sized>(
    committed: &ResponsibilityManifest,
    local: GroupIdentity,
    hint: &RouteHint,
    key: &[u8],
    policy: &P,
) -> Result<(), RoutingError> {
    let input = committed.input();
    if hint.responsibility != input.responsibility || hint.application != input.application {
        return Err(RoutingError::WrongIdentity);
    }
    if hint.scheme != input.scheme || policy.scheme() != input.scheme {
        return Err(RoutingError::UnsupportedSchema);
    }
    if hint.epoch != input.epoch {
        return Err(RoutingError::EpochMismatch);
    }
    let bucket = bucket(policy, key)?;
    if bucket != hint.bucket {
        return Err(RoutingError::InvalidKey);
    }
    let entry = committed.select(bucket)?;
    if hint.scope != entry.scope {
        return Err(RoutingError::OutsideScope);
    }
    if entry.target != RouteTarget::Group(local) || hint.group != local {
        return Err(RoutingError::WrongOwner);
    }
    Ok(())
}
