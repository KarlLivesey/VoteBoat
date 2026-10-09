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
use voteboat::{identity::*, placement::PlacementRequirements, routing::*};

fn responsibility(id: u128) -> ResponsibilityIdentity {
    ResponsibilityIdentity {
        id: ResponsibilityId::new(id).unwrap(),
        incarnation: ResponsibilityIncarnation::new(1).unwrap(),
    }
}
fn group(id: u128) -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(id).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
fn range(start: u16, end: u16) -> BucketRange {
    BucketRange::new(start, end).unwrap()
}
fn scheme() -> PartitionScheme {
    PartitionScheme {
        id: RoutingSchemeId::new(1).unwrap(),
        version: 1,
    }
}
fn input(id: u128, scope: BucketRange, execution: ExecutionMode) -> ManifestInput {
    ManifestInput {
        responsibility: responsibility(id),
        parent: None,
        authority: group(id),
        application: ApplicationAdapter {
            id: ApplicationAdapterId::new(1).unwrap(),
            version: 1,
        },
        scheme: scheme(),
        scope,
        epoch: OwnershipEpoch::new(1).unwrap(),
        generation: RouteGeneration::new(1).unwrap(),
        placement: PlacementRequirements {
            minimum_voting_domains: 3,
            survive_any_single_domain_loss: true,
        },
        state: ResponsibilityState::Active,
        execution,
    }
}
fn manifest(input: ManifestInput) -> ResponsibilityManifest {
    ResponsibilityManifest::new(input).unwrap()
}
fn child(id: u128) -> RouteTarget {
    RouteTarget::Child(ChildAuthority {
        responsibility: responsibility(id),
        group: group(id),
        epoch: OwnershipEpoch::new(1).unwrap(),
    })
}
fn fixture() -> Vec<ResponsibilityManifest> {
    let root = input(
        1,
        range(0, 256),
        ExecutionMode::Delegated(vec![
            RouteEntry {
                scope: range(0, 128),
                target: child(2),
            },
            RouteEntry {
                scope: range(128, 256),
                target: child(3),
            },
        ]),
    );
    let mut orders = input(
        2,
        range(0, 128),
        ExecutionMode::Partitioned(vec![
            RouteEntry {
                scope: range(0, 64),
                target: RouteTarget::Group(group(20)),
            },
            RouteEntry {
                scope: range(64, 128),
                target: RouteTarget::Group(group(21)),
            },
        ]),
    );
    let mut jobs = input(3, range(128, 256), ExecutionMode::Single(group(30)));
    for value in [&mut orders, &mut jobs] {
        value.parent = Some(ParentAuthority {
            responsibility: responsibility(1),
            group: group(1),
        });
    }
    vec![manifest(root), manifest(orders), manifest(jobs)]
}
/// Host-owned immutable directory view; rejects all mutation instead of owning
/// duplicate cache resources. The resolver must work through this public seam.
struct HostView {
    entries: Vec<ResponsibilityManifest>,
    wrong_lookup: bool,
}
impl ManifestCache for HostView {
    fn get(&self, id: ResponsibilityIdentity) -> Option<&ResponsibilityManifest> {
        if self.wrong_lookup {
            self.entries.first()
        } else {
            self.entries.iter().find(|m| m.input().responsibility == id)
        }
    }
    fn admit(
        &mut self,
        value: ResponsibilityManifest,
    ) -> Result<(), (RoutingError, ResponsibilityManifest)> {
        Err((RoutingError::Capacity, value))
    }
    fn invalidate(&mut self, _: ResponsibilityIdentity, _: RouteGeneration) -> bool {
        false
    }
    fn limits(&self) -> ManifestCacheLimits {
        ManifestCacheLimits {
            manifests: 256,
            bytes: MAX_CACHE_BYTES,
        }
    }
    fn usage(&self) -> ManifestCacheUsage {
        ManifestCacheUsage {
            manifests: self.entries.len(),
            bytes: self.entries.iter().map(|m| m.retained_bytes()).sum(),
        }
    }
}
struct HostPolicy;
impl PartitionPolicy for HostPolicy {
    fn scheme(&self) -> PartitionScheme {
        scheme()
    }
    fn bucket(&self, key: &[u8]) -> Result<u16, RoutingError> {
        // Same persistent scheme/encoding as the native byte policy.
        if key.len() != 1 {
            return Err(RoutingError::InvalidKey);
        }
        Ok(key[0].into())
    }
}
fn view() -> HostView {
    HostView {
        entries: fixture(),
        wrong_lookup: false,
    }
}

#[test]
fn host_policy_and_view_resolve_every_bucket_with_per_group_ordering() {
    let cache = view();
    for bucket in 0..=255 {
        let hint = resolve(&cache, &HostPolicy, responsibility(1), &[bucket], 2).unwrap();
        let (resource, owner) = match bucket {
            0..=63 => (2, 20),
            64..=127 => (2, 21),
            _ => (3, 30),
        };
        assert_eq!(hint.responsibility, responsibility(resource));
        assert_eq!(hint.group, group(owner));
        assert!(hint.scope.contains(u16::from(bucket)));
        check_owner(
            cache.get(responsibility(resource)).unwrap(),
            group(owner),
            &hint,
            &[bucket],
            &HostPolicy,
        )
        .unwrap();
    }
    assert_eq!(
        resolve(&cache, &HostPolicy, responsibility(1), &[0], 1),
        Err(RoutingError::HopLimit)
    );
    assert_eq!(
        resolve(&cache, &HostPolicy, responsibility(1), &[0], 0),
        Err(RoutingError::InvalidLimits)
    );
    assert_eq!(
        resolve(&cache, &HostPolicy, responsibility(1), &[0], 33),
        Err(RoutingError::InvalidLimits)
    );
}

#[test]
fn coverage_is_complete_sorted_disjoint_and_rejections_preserve_owned_input() {
    for routes in [
        vec![RouteEntry {
            scope: range(1, 256),
            target: RouteTarget::Group(group(1)),
        }],
        vec![RouteEntry {
            scope: range(0, 255),
            target: RouteTarget::Group(group(1)),
        }],
        vec![
            RouteEntry {
                scope: range(0, 129),
                target: RouteTarget::Group(group(1)),
            },
            RouteEntry {
                scope: range(128, 256),
                target: RouteTarget::Group(group(2)),
            },
        ],
        vec![
            RouteEntry {
                scope: range(128, 256),
                target: RouteTarget::Group(group(2)),
            },
            RouteEntry {
                scope: range(0, 128),
                target: RouteTarget::Group(group(1)),
            },
        ],
    ] {
        let value = input(1, range(0, 256), ExecutionMode::Partitioned(routes));
        let expected = value.clone();
        let (error, returned) = ResponsibilityManifest::new(value).unwrap_err();
        assert_eq!(error, RoutingError::Coverage);
        assert_eq!(returned, expected);
    }
    for (start, end) in [(0, 0), (200, 100), (0, 257), (256, 257)] {
        assert_eq!(
            BucketRange::new(start, end),
            Err(RoutingError::InvalidRange)
        );
    }
    let mut routes = Vec::with_capacity(MAX_MANIFEST_ROUTES + 1);
    routes.push(RouteEntry {
        scope: range(0, 256),
        target: RouteTarget::Group(group(1)),
    });
    let value = input(1, range(0, 256), ExecutionMode::Partitioned(routes));
    let (_, returned) = ResponsibilityManifest::new(value).unwrap_err();
    let ExecutionMode::Partitioned(routes) = returned.execution else {
        panic!()
    };
    assert_eq!(routes.capacity(), MAX_MANIFEST_ROUTES + 1);
}

#[test]
fn malformed_modes_schemas_and_direct_cycles_are_refused() {
    let mut cases = Vec::new();
    let mut value = input(1, range(0, 256), ExecutionMode::Single(group(1)));
    value.scheme.version = 0;
    cases.push((value, RoutingError::UnsupportedSchema));
    let mut value = input(1, range(0, 256), ExecutionMode::Single(group(1)));
    value.placement.minimum_voting_domains = 0;
    cases.push((value, RoutingError::InvalidPlacement));
    cases.push((
        input(
            1,
            range(0, 256),
            ExecutionMode::Delegated(vec![RouteEntry {
                scope: range(0, 256),
                target: child(1),
            }]),
        ),
        RoutingError::Cycle,
    ));
    cases.push((
        input(
            1,
            range(0, 256),
            ExecutionMode::Partitioned(vec![RouteEntry {
                scope: range(0, 256),
                target: child(2),
            }]),
        ),
        RoutingError::InvalidMode,
    ));
    cases.push((
        input(
            1,
            range(0, 256),
            ExecutionMode::Delegated(vec![
                RouteEntry {
                    scope: range(0, 128),
                    target: child(2),
                },
                RouteEntry {
                    scope: range(128, 256),
                    target: child(2),
                },
            ]),
        ),
        RoutingError::DuplicateChild,
    ));
    for (value, expected) in cases {
        assert_eq!(ResponsibilityManifest::new(value).unwrap_err().0, expected);
    }
}

#[test]
fn cache_provider_and_delegation_bindings_are_checked_by_core() {
    let mut cache = view();
    cache.wrong_lookup = true;
    assert_eq!(
        resolve(&cache, &HostPolicy, responsibility(2), &[0], 2),
        Err(RoutingError::WrongIdentity)
    );
    cache.wrong_lookup = false;
    for field in 0..5 {
        let mut changed = cache.entries[1].clone().into_input();
        match field {
            0 => changed.parent = None,
            1 => changed.authority = group(99),
            2 => changed.epoch = OwnershipEpoch::new(2).unwrap(),
            3 => {
                changed.scope = range(0, 129);
                changed.execution = ExecutionMode::Single(group(20));
            }
            _ => changed.scheme.version = 2,
        }
        let old = std::mem::replace(&mut cache.entries[1], manifest(changed));
        let expected = match field {
            0 => RoutingError::WrongParent,
            4 => RoutingError::UnsupportedSchema,
            _ => RoutingError::WrongChild,
        };
        assert_eq!(
            resolve(&cache, &HostPolicy, responsibility(1), &[0], 2),
            Err(expected)
        );
        cache.entries[1] = old;
    }
    cache.entries.remove(1);
    assert_eq!(
        resolve(&cache, &HostPolicy, responsibility(1), &[0], 2),
        Err(RoutingError::Missing(responsibility(2)))
    );
}

#[test]
fn recursive_cycle_and_out_of_range_host_policy_fail_closed() {
    let mut a = input(
        1,
        range(0, 256),
        ExecutionMode::Delegated(vec![RouteEntry {
            scope: range(0, 256),
            target: child(2),
        }]),
    );
    let mut b = input(
        2,
        range(0, 256),
        ExecutionMode::Delegated(vec![RouteEntry {
            scope: range(0, 256),
            target: child(1),
        }]),
    );
    a.parent = Some(ParentAuthority {
        responsibility: responsibility(2),
        group: group(2),
    });
    b.parent = Some(ParentAuthority {
        responsibility: responsibility(1),
        group: group(1),
    });
    let cache = HostView {
        entries: vec![manifest(a), manifest(b)],
        wrong_lookup: false,
    };
    assert_eq!(
        resolve(&cache, &HostPolicy, responsibility(1), &[0], 32),
        Err(RoutingError::Cycle)
    );
    struct BadPolicy;
    impl PartitionPolicy for BadPolicy {
        fn scheme(&self) -> PartitionScheme {
            scheme()
        }
        fn bucket(&self, _: &[u8]) -> Result<u16, RoutingError> {
            Ok(256)
        }
    }
    assert_eq!(
        resolve(&cache, &BadPolicy, responsibility(1), &[0], 2),
        Err(RoutingError::InvalidKey)
    );
    assert_eq!(
        resolve(
            &cache,
            &HostPolicy,
            responsibility(1),
            &vec![0; MAX_ROUTING_KEY_BYTES + 1],
            2
        ),
        Err(RoutingError::InvalidKey)
    );
}

#[test]
fn local_owner_rechecks_command_context_and_fence_even_with_warm_route() {
    let cache = view();
    let hint = resolve(&cache, &HostPolicy, responsibility(1), &[10], 2).unwrap();
    let local = cache.get(responsibility(2)).unwrap();
    // Admission succeeds before the fence.
    check_owner(local, group(20), &hint, &[10], &HostPolicy).unwrap();
    // Applying the same queued command after a local committed fence refuses.
    let mut fenced = local.clone().into_input();
    fenced.state = ResponsibilityState::Fenced;
    assert_eq!(
        check_owner(&manifest(fenced), group(20), &hint, &[10], &HostPolicy),
        Err(RoutingError::Fenced)
    );
    let mut newer = local.clone().into_input();
    newer.epoch = OwnershipEpoch::new(2).unwrap();
    assert_eq!(
        check_owner(&manifest(newer), group(20), &hint, &[10], &HostPolicy),
        Err(RoutingError::EpochMismatch)
    );
    for field in 0..6 {
        let mut forged = hint;
        match field {
            0 => forged.group = group(21),
            1 => forged.responsibility.incarnation = ResponsibilityIncarnation::new(2).unwrap(),
            2 => forged.scope = range(0, 128),
            3 => forged.bucket = 11,
            4 => forged.scheme.version = 2,
            _ => forged.application.version = 2,
        }
        assert!(check_owner(local, group(20), &forged, &[10], &HostPolicy).is_err());
    }
    assert_eq!(
        check_owner(local, group(21), &hint, &[10], &HostPolicy),
        Err(RoutingError::WrongOwner)
    );
    let mut refreshed = local.clone().into_input();
    refreshed.generation = RouteGeneration::new(2).unwrap();
    check_owner(&manifest(refreshed), group(20), &hint, &[10], &HostPolicy).unwrap();
}

#[cfg(feature = "native")]
mod native {
    use super::*;
    use voteboat::native::routing::*;
    fn make_cache(count: usize, bytes: usize) -> NativeManifestCache {
        NativeManifestCache::new(ManifestCacheLimits {
            manifests: count,
            bytes,
        })
        .unwrap()
    }
    #[test]
    fn native_cache_and_policy_preserve_warm_child_without_parent() {
        let mut cache = make_cache(3, MAX_CACHE_BYTES);
        for manifest in fixture() {
            cache.admit(manifest).unwrap();
        }
        for byte in 0..=255 {
            assert_eq!(
                resolve(&cache, &NativeBytePartition, responsibility(1), &[byte], 2),
                resolve(&view(), &HostPolicy, responsibility(1), &[byte], 2)
            );
        }
        let hint = resolve(&cache, &NativeBytePartition, responsibility(1), &[12], 2).unwrap();
        assert!(cache.invalidate(responsibility(1), RouteGeneration::new(1).unwrap()));
        assert_eq!(
            resolve(&cache, &NativeBytePartition, responsibility(1), &[12], 2),
            Err(RoutingError::Missing(responsibility(1)))
        );
        assert_eq!(
            resolve(&cache, &NativeBytePartition, responsibility(2), &[12], 1),
            Ok(hint)
        );
        let second = make_cache(3, MAX_CACHE_BYTES);
        drop(cache);
        assert_eq!(second.usage().manifests, 0);
        assert!(NativeBytePartition.bucket(&[]).is_err());
        assert!(NativeBytePartition.bucket(&[1, 2]).is_err());
    }
    #[test]
    fn generation_epoch_lineage_conflicts_and_stale_invalidation_are_atomic() {
        let first = fixture().remove(1);
        let mut cache = make_cache(1, MAX_CACHE_BYTES);
        cache.admit(first.clone()).unwrap();
        cache.admit(first.clone()).unwrap();
        let initial = cache.usage();
        let mut next = first.clone().into_input();
        next.generation = RouteGeneration::new(2).unwrap();
        cache.admit(manifest(next.clone())).unwrap();
        assert_eq!(cache.usage(), initial);
        assert!(!cache.invalidate(responsibility(2), RouteGeneration::new(1).unwrap()));
        let cases = [
            (first, RoutingError::StaleGeneration),
            (
                {
                    let mut v = next.clone();
                    v.state = ResponsibilityState::Fenced;
                    manifest(v)
                },
                RoutingError::GenerationConflict,
            ),
            (
                {
                    let mut v = next.clone();
                    v.generation = RouteGeneration::new(3).unwrap();
                    v.parent = None;
                    manifest(v)
                },
                RoutingError::IdentityChange,
            ),
            (
                {
                    let mut v = next.clone();
                    v.generation = RouteGeneration::new(3).unwrap();
                    v.execution = ExecutionMode::Single(group(99));
                    manifest(v)
                },
                RoutingError::EpochMismatch,
            ),
        ];
        for (value, error) in cases {
            let expected = value.clone();
            assert_eq!(cache.admit(value), Err((error, expected)));
            assert_eq!(cache.usage(), initial);
            assert_eq!(cache.get(responsibility(2)).unwrap().input(), &next);
        }
        next.generation = RouteGeneration::new(3).unwrap();
        next.epoch = OwnershipEpoch::new(2).unwrap();
        cache.admit(manifest(next.clone())).unwrap();
        let mut old_epoch = next.clone();
        old_epoch.generation = RouteGeneration::new(4).unwrap();
        old_epoch.epoch = OwnershipEpoch::new(1).unwrap();
        assert_eq!(
            cache.admit(manifest(old_epoch)).unwrap_err().0,
            RoutingError::EpochRegression
        );
        next.generation = RouteGeneration::new(4).unwrap();
        next.state = ResponsibilityState::Fenced;
        cache.admit(manifest(next.clone())).unwrap();
        next.generation = RouteGeneration::new(5).unwrap();
        next.state = ResponsibilityState::Active;
        assert_eq!(
            cache.admit(manifest(next)).unwrap_err().0,
            RoutingError::Fenced
        );
        assert!(cache.invalidate(responsibility(2), RouteGeneration::new(4).unwrap()));
        assert_eq!(
            cache.usage(),
            ManifestCacheUsage {
                manifests: 0,
                bytes: 0
            }
        );
    }
    #[test]
    fn capacity_rejection_returns_original_allocation_and_replacement_accounts_spare_capacity() {
        let first = fixture().remove(1);
        let bytes = first.retained_bytes();
        let mut cache = make_cache(1, bytes);
        cache.admit(first.clone()).unwrap();
        let mut next = first.clone().into_input();
        next.generation = RouteGeneration::new(2).unwrap();
        let ExecutionMode::Partitioned(ref mut entries) = next.execution else {
            panic!()
        };
        entries.reserve_exact(10);
        let ptr = entries.as_ptr();
        let cap = entries.capacity();
        let (error, returned) = cache.admit(manifest(next)).unwrap_err();
        assert_eq!(error, RoutingError::Capacity);
        let ExecutionMode::Partitioned(entries) = &returned.input().execution else {
            panic!()
        };
        assert_eq!(entries.as_ptr(), ptr);
        assert_eq!(entries.capacity(), cap);
        assert_eq!(cache.usage().bytes, bytes);
        assert_eq!(cache.get(responsibility(2)), Some(&first));
        assert_eq!(
            cache.admit(fixture().remove(2)).unwrap_err().0,
            RoutingError::Capacity
        );
        for limits in [
            ManifestCacheLimits {
                manifests: 0,
                bytes: 1,
            },
            ManifestCacheLimits {
                manifests: MAX_CACHE_MANIFESTS + 1,
                bytes: 1,
            },
            ManifestCacheLimits {
                manifests: 1,
                bytes: 0,
            },
            ManifestCacheLimits {
                manifests: 1,
                bytes: MAX_CACHE_BYTES + 1,
            },
        ] {
            assert_eq!(
                NativeManifestCache::new(limits).unwrap_err(),
                RoutingError::InvalidLimits
            );
        }
    }
}

#[test]
fn recursive_paths_use_the_exact_visit_budget_and_composite_scopes() {
    let chain = |depth: u128| {
        let mut entries = Vec::new();
        for id in 1..=depth {
            let execution = if id == depth {
                ExecutionMode::Single(group(900))
            } else {
                ExecutionMode::Delegated(vec![RouteEntry {
                    scope: range(0, 256),
                    target: child(id + 1),
                }])
            };
            let mut value = input(id, range(0, 256), execution);
            if id > 1 {
                value.parent = Some(ParentAuthority {
                    responsibility: responsibility(id - 1),
                    group: group(id - 1),
                });
            }
            entries.push(manifest(value));
        }
        HostView {
            entries,
            wrong_lookup: false,
        }
    };
    let cache = chain(32);
    let hint = resolve(&cache, &HostPolicy, responsibility(1), &[255], 32).unwrap();
    assert_eq!(hint.responsibility, responsibility(32));
    assert_eq!(hint.group, group(900));
    assert_eq!(
        resolve(&chain(33), &HostPolicy, responsibility(1), &[255], 32),
        Err(RoutingError::HopLimit)
    );
    let mut entries = fixture();
    let mut root = entries[0].clone().into_input();
    root.execution = ExecutionMode::Delegated(vec![
        RouteEntry {
            scope: range(0, 128),
            target: RouteTarget::Group(group(50)),
        },
        RouteEntry {
            scope: range(128, 256),
            target: child(3),
        },
    ]);
    entries[0] = manifest(root);
    let cache = HostView {
        entries,
        wrong_lookup: false,
    };
    let local = resolve(&cache, &HostPolicy, responsibility(1), &[127], 1).unwrap();
    assert_eq!(local.group, group(50));
    check_owner(&cache.entries[0], group(50), &local, &[127], &HostPolicy).unwrap();
    assert_eq!(
        resolve(&cache, &HostPolicy, responsibility(1), &[128], 2)
            .unwrap()
            .group,
        group(30)
    );
}

#[cfg(feature = "native")]
#[test]
fn cache_accounts_successful_replacement_and_separate_incarnations() {
    use voteboat::native::routing::*;
    let mut cache = NativeManifestCache::new(ManifestCacheLimits {
        manifests: 2,
        bytes: MAX_CACHE_BYTES,
    })
    .unwrap();
    let first = fixture().remove(1);
    cache.admit(first.clone()).unwrap();
    let mut next = first.into_input();
    next.generation = RouteGeneration::new(2).unwrap();
    let ExecutionMode::Partitioned(ref mut entries) = next.execution else {
        panic!()
    };
    entries.reserve_exact(10);
    let next = manifest(next);
    let expected_bytes = next.retained_bytes();
    cache.admit(next).unwrap();
    assert_eq!(
        cache.usage(),
        ManifestCacheUsage {
            manifests: 1,
            bytes: expected_bytes
        }
    );
    let mut new_incarnation = cache.get(responsibility(2)).unwrap().clone().into_input();
    new_incarnation.responsibility.incarnation = ResponsibilityIncarnation::new(2).unwrap();
    let new_identity = new_incarnation.responsibility;
    let new_manifest = manifest(new_incarnation);
    let total = expected_bytes + new_manifest.retained_bytes();
    cache.admit(new_manifest).unwrap();
    assert_eq!(
        cache.usage(),
        ManifestCacheUsage {
            manifests: 2,
            bytes: total
        }
    );
    assert!(cache.invalidate(new_identity, RouteGeneration::new(2).unwrap()));
    assert_eq!(cache.usage().bytes, expected_bytes);
    assert!(cache.get(responsibility(2)).is_some());
}

#[derive(Clone)]
struct HostDiscovery {
    entries: Vec<ResponsibilityManifest>,
    calls: std::rc::Rc<std::cell::Cell<usize>>,
    closed: bool,
    expired: bool,
    wrong_source: bool,
    wrong_manifest: bool,
}
impl ManifestDiscovery for HostDiscovery {
    fn lookup(
        &mut self,
        request: ManifestLookup,
        _: voteboat::runtime::MonoTime,
    ) -> Result<ManifestObservation, ManifestDiscoveryError> {
        self.calls.set(self.calls.get() + 1);
        if self.closed {
            return Err(ManifestDiscoveryError::Closed);
        }
        let manifest = self
            .entries
            .iter()
            .find(|m| m.input().responsibility == request.locator.responsibility)
            .ok_or(ManifestDiscoveryError::Missing)?
            .clone();
        let locator = if self.wrong_source {
            AuthorityLocator {
                authority: group(999),
                ..request.locator
            }
        } else {
            request.locator
        };
        Ok(ManifestObservation {
            locator,
            observation: ManifestObservationId::new(1).unwrap(),
            manifest: if self.wrong_manifest {
                self.entries[0].clone()
            } else {
                manifest
            },
            expires_at: voteboat::runtime::MonoTime(if self.expired { 0 } else { 100 }),
        })
    }
    fn invalidate(&mut self, _: AuthorityLocator, _: ManifestObservationId) -> bool {
        false
    }
    fn close(&mut self) {
        self.closed = true;
    }
}
struct FetchCache {
    view: HostView,
    capacity: usize,
}
impl ManifestCache for FetchCache {
    fn get(&self, id: ResponsibilityIdentity) -> Option<&ResponsibilityManifest> {
        self.view.get(id)
    }
    fn admit(
        &mut self,
        value: ResponsibilityManifest,
    ) -> Result<(), (RoutingError, ResponsibilityManifest)> {
        if self.view.entries.len() >= self.capacity {
            return Err((RoutingError::Capacity, value));
        }
        self.view.entries.push(value);
        Ok(())
    }
    fn invalidate(&mut self, id: ResponsibilityIdentity, g: RouteGeneration) -> bool {
        let Some(i) = self
            .view
            .entries
            .iter()
            .position(|m| m.input().responsibility == id && m.input().generation == g)
        else {
            return false;
        };
        self.view.entries.remove(i);
        true
    }
    fn limits(&self) -> ManifestCacheLimits {
        self.view.limits()
    }
    fn usage(&self) -> ManifestCacheUsage {
        self.view.usage()
    }
}
fn fetch_fixture() -> (FetchCache, HostDiscovery) {
    (
        FetchCache {
            view: HostView {
                entries: vec![],
                wrong_lookup: false,
            },
            capacity: 3,
        },
        HostDiscovery {
            entries: fixture(),
            calls: Default::default(),
            closed: false,
            expired: false,
            wrong_source: false,
            wrong_manifest: false,
        },
    )
}
fn fetch_request(id: u128, key: &[u8], lookups: usize) -> DiscoverRouteRequest<'_> {
    DiscoverRouteRequest {
        start: ManifestLookup {
            locator: AuthorityLocator {
                responsibility: responsibility(id),
                authority: group(id),
            },
            minimum_epoch: None,
            minimum_generation: None,
        },
        key,
        max_hops: 2,
        lookups,
        now: voteboat::runtime::MonoTime(0),
    }
}
#[test]
fn downstream_authority_discovery_fills_only_needed_path_and_cached_child_ignores_closed_source() {
    let (mut cache, mut source) = fetch_fixture();
    let hint = resolve_discovered(
        &mut cache,
        &HostPolicy,
        &mut source,
        fetch_request(1, &[10], 2),
    )
    .unwrap();
    assert_eq!(hint.group, group(20));
    assert_eq!(source.calls.get(), 2);
    assert!(cache.get(responsibility(3)).is_none());
    let mut shared = source.clone();
    source.close();
    assert!(shared
        .lookup(
            fetch_request(1, &[10], 1).start,
            voteboat::runtime::MonoTime(0)
        )
        .is_ok());
    shared.close();
    cache.invalidate(responsibility(1), RouteGeneration::new(1).unwrap());
    assert_eq!(
        resolve_discovered(
            &mut cache,
            &HostPolicy,
            &mut source,
            fetch_request(2, &[10], 0)
        )
        .unwrap(),
        hint
    );
    assert_eq!(source.calls.get(), 3, "cache hit performs no source call");
    let mut stale = fetch_request(2, &[10], 0);
    stale.start.minimum_epoch = Some(OwnershipEpoch::new(2).unwrap());
    assert_eq!(
        resolve_discovered(&mut cache, &HostPolicy, &mut source, stale),
        Err(ManifestDiscoveryError::Routing(
            RoutingError::EpochRegression
        ))
    );
    let mut stale_generation = fetch_request(2, &[10], 0);
    stale_generation.start.minimum_generation = Some(RouteGeneration::new(2).unwrap());
    assert_eq!(
        resolve_discovered(&mut cache, &HostPolicy, &mut source, stale_generation),
        Err(ManifestDiscoveryError::Routing(
            RoutingError::StaleGeneration
        ))
    );
    cache.invalidate(responsibility(2), RouteGeneration::new(1).unwrap());
    assert_eq!(
        resolve_discovered(
            &mut cache,
            &HostPolicy,
            &mut source,
            fetch_request(2, &[10], 0)
        ),
        Err(ManifestDiscoveryError::BudgetExhausted(responsibility(2)))
    );
}
#[test]
fn discovered_manifests_validate_source_identity_expiry_budget_and_fenced_state() {
    for mode in 0..3 {
        let (mut cache, mut source) = fetch_fixture();
        source.expired = mode == 0;
        source.wrong_source = mode == 1;
        source.wrong_manifest = mode == 2;
        let error = resolve_discovered(
            &mut cache,
            &HostPolicy,
            &mut source,
            fetch_request(2, &[10], 1),
        )
        .unwrap_err();
        assert_eq!(
            error,
            match mode {
                0 => ManifestDiscoveryError::Expired,
                1 => ManifestDiscoveryError::WrongAuthority,
                _ => ManifestDiscoveryError::WrongIdentity,
            }
        );
        assert_eq!(cache.usage().manifests, 0);
    }
    let (mut cache, mut source) = fetch_fixture();
    assert_eq!(
        resolve_discovered(
            &mut cache,
            &HostPolicy,
            &mut source,
            fetch_request(1, &[10], 1)
        ),
        Err(ManifestDiscoveryError::BudgetExhausted(responsibility(2)))
    );
    assert_eq!(source.calls.get(), 1);
    let mut fenced = source.entries[1].clone().into_input();
    fenced.state = ResponsibilityState::Fenced;
    source.entries[1] = manifest(fenced);
    assert_eq!(
        resolve_discovered(
            &mut cache,
            &HostPolicy,
            &mut source,
            fetch_request(1, &[10], 1)
        ),
        Err(ManifestDiscoveryError::Routing(RoutingError::Fenced))
    );
    assert_eq!(
        cache.get(responsibility(2)).unwrap().input().state,
        ResponsibilityState::Fenced
    );
    let (mut cache, mut source) = fetch_fixture();
    cache.capacity = 1;
    assert_eq!(
        resolve_discovered(
            &mut cache,
            &HostPolicy,
            &mut source,
            fetch_request(1, &[10], 2)
        ),
        Err(ManifestDiscoveryError::Routing(RoutingError::Capacity))
    );
    assert_eq!(cache.usage().manifests, 1);
    assert_eq!(source.entries.len(), 3);
}
