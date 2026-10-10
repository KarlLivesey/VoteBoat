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
mod discovery;
pub use discovery::*;

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
    /// Explicitly unowned delegated selector. It grants no data/child authority.
    Vacant,
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
        let mut vacancies = 0;
        for entry in entries {
            if entry.scope.start != end || entry.scope.end > input.scope.end {
                return Err(RoutingError::Coverage);
            }
            end = entry.scope.end;
            if entry.target == RouteTarget::Vacant {
                if !delegated {
                    return Err(RoutingError::InvalidMode);
                }
                vacancies += 1;
            }
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
        if delegated && children.is_empty() && vacancies == 0 {
            return Err(RoutingError::InvalidMode);
        }
        Ok(())
    }
    pub fn has_vacancies(&self) -> bool {
        matches!(&self.0.execution, ExecutionMode::Delegated(v) if v.iter().any(|r|r.target==RouteTarget::Vacant))
    }
    pub fn input(&self) -> &ManifestInput {
        &self.0
    }
    pub fn into_input(self) -> ManifestInput {
        self.0
    }
    /// A newer authenticated routing view may advance child locator epochs
    /// without moving this parent's own ownership. No range, group or child
    /// identity can change through this metadata-only refresh.
    pub fn refreshes_child_epochs(&self, previous: &Self) -> bool {
        let next = self.input();
        let old = previous.input();
        if next.responsibility != old.responsibility
            || next.parent != old.parent
            || next.authority != old.authority
            || next.application != old.application
            || next.scheme != old.scheme
            || next.scope != old.scope
            || next.epoch != old.epoch
            || next.state != old.state
            || next.generation <= old.generation
        {
            return false;
        }
        let (ExecutionMode::Delegated(a), ExecutionMode::Delegated(b)) =
            (&next.execution, &old.execution)
        else {
            return false;
        };
        a.len() == b.len()
            && a.iter().zip(b).all(|(a, b)| {
                if a.scope != b.scope {
                    return false;
                }
                match (a.target, b.target) {
                    (RouteTarget::Group(a), RouteTarget::Group(b)) => a == b,
                    (RouteTarget::Vacant, RouteTarget::Vacant) => true,
                    (RouteTarget::Child(a), RouteTarget::Child(b)) => {
                        a.responsibility == b.responsibility
                            && a.group == b.group
                            && a.epoch >= b.epoch
                    }
                    _ => false,
                }
            })
    }
    /// Recognizes a newer trusted directory view that removes child routes.
    /// This shape check is not deletion evidence: the directory must commit
    /// retirement through deletion evidence or a checked reparenting transition.
    pub fn retires_child_slots(&self, previous: &Self) -> bool {
        let next = self.input();
        let old = previous.input();
        if next.responsibility != old.responsibility
            || next.parent != old.parent
            || next.authority != old.authority
            || next.application != old.application
            || next.scheme != old.scheme
            || next.scope != old.scope
            || next.placement != old.placement
            || next.epoch != old.epoch
            || next.state != ResponsibilityState::Active
            || old.state != ResponsibilityState::Active
            || next.generation <= old.generation
        {
            return false;
        }
        let (ExecutionMode::Delegated(a), ExecutionMode::Delegated(b)) =
            (&next.execution, &old.execution)
        else {
            return false;
        };
        a.len() == b.len()
            && a.iter().zip(b).any(|(a, b)| {
                a.target == RouteTarget::Vacant && matches!(b.target, RouteTarget::Child(_))
            })
            && a.iter().zip(b).all(|(a, b)| {
                a.scope == b.scope
                    && match (a.target, b.target) {
                        (RouteTarget::Vacant, RouteTarget::Child(_)) => true,
                        (RouteTarget::Child(a), RouteTarget::Child(b)) => {
                            a.responsibility == b.responsibility
                                && a.group == b.group
                                && a.epoch >= b.epoch
                        }
                        (a, b) => a == b,
                    }
            })
    }
    /// Shape of a trusted metadata publication filling vacant child selectors.
    /// This grants no serving authority to the child data group by itself.
    pub fn fills_vacant_child_slots(&self, previous: &Self) -> bool {
        let (a, b) = (self.input(), previous.input());
        if a.responsibility != b.responsibility
            || a.parent != b.parent
            || a.authority != b.authority
            || a.application != b.application
            || a.scheme != b.scheme
            || a.scope != b.scope
            || a.placement != b.placement
            || a.epoch != b.epoch
            || a.generation <= b.generation
            || a.state != ResponsibilityState::Active
            || b.state != ResponsibilityState::Active
        {
            return false;
        }
        let (ExecutionMode::Delegated(a), ExecutionMode::Delegated(b)) =
            (&a.execution, &b.execution)
        else {
            return false;
        };
        a.len() == b.len()
            && a.iter().zip(b).any(|(a, b)| {
                matches!(a.target, RouteTarget::Child(_)) && b.target == RouteTarget::Vacant
            })
            && a.iter().zip(b).all(|(a, b)| {
                a.scope == b.scope
                    && match (a.target, b.target) {
                        (RouteTarget::Child(_), RouteTarget::Vacant) => true,
                        (RouteTarget::Child(a), RouteTarget::Child(b)) => {
                            a.responsibility == b.responsibility
                                && a.group == b.group
                                && a.epoch >= b.epoch
                        }
                        (a, b) => a == b,
                    }
            })
    }
    /// Trusted same-authority parent rebinding preserves data ownership and all
    /// descendant selectors. Only the parent pointer and generation can change.
    pub fn reparents_within_authority(&self, previous: &Self) -> bool {
        let (a, b) = (self.input(), previous.input());
        a.parent.is_some_and(|p| p.group == a.authority)
            && b.parent.is_some_and(|p| p.group == a.authority)
            && self.reparents_preserving_owner(previous)
    }
    /// Trusted parent rebinding across metadata authorities. Data ownership,
    /// epoch and every descendant selector must remain unchanged.
    pub fn reparents_preserving_owner(&self, previous: &Self) -> bool {
        let (a, b) = (self.input(), previous.input());
        a.parent != b.parent
            && a.parent.is_some()
            && b.parent.is_some()
            && a.responsibility == b.responsibility
            && a.authority == b.authority
            && a.application == b.application
            && a.scheme == b.scheme
            && a.scope == b.scope
            && a.placement == b.placement
            && a.epoch == b.epoch
            && a.generation > b.generation
            && a.state == ResponsibilityState::Active
            && b.state == ResponsibilityState::Active
            && a.execution == b.execution
    }
    /// Exact trusted metadata relocation: local references follow the authority,
    /// while concrete data owners, epochs, selectors and all other fields stay put.
    /// This predicate validates structure, not remote commitment or authorization.
    pub fn moves_metadata_from(&self, previous: &Self) -> bool {
        let (a, b) = (self.input(), previous.input());
        if a.authority.id == b.authority.id
            || b.generation.get().checked_add(1) != Some(a.generation.get())
        {
            return false;
        }
        let mut expected = b.clone();
        expected.authority = a.authority;
        expected.generation = a.generation;
        if let Some(parent) = &mut expected.parent {
            if parent.group == b.authority {
                parent.group = a.authority;
            }
        }
        if let ExecutionMode::Delegated(routes) = &mut expected.execution {
            for route in routes {
                if let RouteTarget::Child(child) = &mut route.target {
                    if child.group == b.authority {
                        child.group = a.authority;
                    }
                }
            }
        }
        a == &expected
    }
    /// Exact foreign locator refresh for one metadata source/destination pair.
    /// Hosts authenticate the move; this is only a structural cache check.
    pub fn refreshes_metadata_locators(&self, previous: &Self) -> bool {
        let (next, old) = (self.input(), previous.input());
        if old.state != ResponsibilityState::Active
            || old.generation.get().checked_add(1) != Some(next.generation.get())
        {
            return false;
        }
        let mut pair = None;
        let mut change = |from: GroupIdentity, to: GroupIdentity| {
            if from.id == to.id || from.id == old.authority.id || to.id == old.authority.id {
                return false;
            }
            match pair {
                Some(p) => p == (from, to),
                None => {
                    pair = Some((from, to));
                    true
                }
            }
        };
        let mut expected = old.clone();
        expected.generation = next.generation;
        if let (Some(a), Some(b)) = (old.parent, next.parent) {
            if a.group != b.group {
                if !change(a.group, b.group) {
                    return false;
                }
                expected.parent.as_mut().unwrap().group = b.group;
            }
        }
        if let (ExecutionMode::Delegated(a), ExecutionMode::Delegated(b)) =
            (&mut expected.execution, &next.execution)
        {
            if a.len() != b.len() {
                return false;
            }
            for (a, b) in a.iter_mut().zip(b) {
                if let (RouteTarget::Child(from), RouteTarget::Child(to)) =
                    (&mut a.target, b.target)
                {
                    if from.group != to.group {
                        if !change(from.group, to.group) {
                            return false;
                        }
                        from.group = to.group;
                    }
                }
            }
        }
        let Some((from, to)) = pair else {
            return false;
        };
        // Every reference to this source in the manifest must follow the move,
        // including references whose omission would leave a mixed local view.
        if let Some(parent) = &mut expected.parent {
            if parent.group == from {
                parent.group = to;
            }
        }
        if let ExecutionMode::Delegated(routes) = &mut expected.execution {
            for route in routes {
                if let RouteTarget::Child(child) = &mut route.target {
                    if child.group == from {
                        child.group = to;
                    }
                }
            }
        }
        next == &expected
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
    pub(crate) fn select(&self, bucket: u16) -> Result<RouteEntry, RoutingError> {
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
                .ok_or(RoutingError::Coverage)
                .and_then(|r| {
                    if r.target == RouteTarget::Vacant {
                        Err(RoutingError::Vacant)
                    } else {
                        Ok(r)
                    }
                }),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RoutingError {
    Vacant,
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
            RouteTarget::Vacant => return Err(RoutingError::Vacant),
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
