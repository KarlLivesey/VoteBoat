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
#[path = "transfer_source/fixtures.rs"]
mod fixture;
mod support;
use fixture::*;
use voteboat::{
    application::*, bucket_counter::*, deletion::*, directory::*, identity::*, reparenting::*,
    routed::*, routing::*, transfer::*,
};
fn commit<A: StateMachine>(a: &mut A, id: u128, b: Vec<u8>) -> A::Receipt {
    a.apply_batch(&[entry(a.applied_index() + 1, id, b)])
        .unwrap()
        .remove(0)
}
fn fresh(ms: Vec<ResponsibilityManifest>, operations: usize) -> Directory {
    Directory::new(
        DirectoryPlan::new(ms[0].input().authority, ms).unwrap(),
        DirectoryLimits {
            operations,
            history_bytes: 200000,
        },
    )
    .unwrap()
    .with_local_reparenting()
    .unwrap_or_else(|_| panic!("schema11"))
}
fn initial(ms: Vec<ResponsibilityManifest>, operations: usize) -> Directory {
    let mut d = fresh(ms.clone(), operations);
    let b = d.bootstrap_command(200000).unwrap();
    commit(&mut d, 1000, b);
    for (i, m) in ms.into_iter().enumerate() {
        assert!(matches!(
            commit(
                &mut d,
                1001 + i as u128,
                DirectoryCommand {
                    expected: None,
                    manifest: m,
                }
                .encode(200000)
                .unwrap(),
            )
            .outcome,
            DirectoryOutcome::Published(_)
        ));
    }
    d
}
fn recover(d: &Directory) -> Directory {
    let mut next = fresh(
        d.plan().manifests().cloned().collect(),
        d.limits().operations,
    );
    next.restore_checkpoint(11, d.applied_index(), &d.checkpoint(2000000).unwrap())
        .unwrap();
    next
}
fn owner(
    m: ResponsibilityManifest,
    g: u128,
    scope: BucketRange,
) -> RoutedApplication<BucketCounter<Policy>, Policy> {
    RoutedApplication::new(
        group(g),
        m,
        BucketCounter::new(scope, Policy, bucket_limits()).unwrap(),
        Policy,
        RoutedLimits {
            operations: 32,
            semantic_bytes: 8192,
            payload_bytes: 1024,
            inner_checkpoint_bytes: bucket_limits().checkpoint_bound().unwrap(),
        },
    )
    .unwrap_or_else(|e| panic!("{:?}", e.error))
}
fn tree(foreign: bool) -> (ResponsibilityManifest, ResponsibilityManifest) {
    let mut child = grant().into_input();
    child.responsibility.id = ResponsibilityId::new(11).unwrap();
    child.parent = Some(ParentAuthority {
        responsibility: grant().input().responsibility,
        group: group(1),
    });
    child.authority = group(if foreign { 2 } else { 1 });
    child.scope = range(0, 128);
    child.execution = ExecutionMode::Single(group(21));
    let child = ResponsibilityManifest::new(child).unwrap();
    let mut parent = grant().into_input();
    parent.execution = ExecutionMode::Delegated(vec![
        RouteEntry {
            scope: range(0, 128),
            target: RouteTarget::Child(ChildAuthority {
                responsibility: child.input().responsibility,
                group: child.input().authority,
                epoch: child.input().epoch,
            }),
        },
        RouteEntry {
            scope: range(128, 256),
            target: RouteTarget::Group(group(20)),
        },
    ]);
    (ResponsibilityManifest::new(parent).unwrap(), child)
}
fn fixture() -> (ReparentPlan, Vec<ResponsibilityManifest>) {
    let (old, child) = tree(false);
    let mut new = grant().into_input();
    new.responsibility.id = ResponsibilityId::new(30).unwrap();
    new.execution = ExecutionMode::Delegated(vec![
        RouteEntry {
            scope: range(0, 128),
            target: RouteTarget::Vacant,
        },
        RouteEntry {
            scope: range(128, 256),
            target: RouteTarget::Group(group(40)),
        },
    ]);
    let new = ResponsibilityManifest::new(new).unwrap();
    let plan = ReparentPlan::new(old.clone(), new.clone(), child.clone()).unwrap();
    (plan, vec![old, new, child])
}
struct View(Vec<ResponsibilityManifest>);
impl ManifestCache for View {
    fn get(&self, id: ResponsibilityIdentity) -> Option<&ResponsibilityManifest> {
        self.0.iter().find(|m| m.input().responsibility == id)
    }
    fn admit(
        &mut self,
        m: ResponsibilityManifest,
    ) -> Result<(), (RoutingError, ResponsibilityManifest)> {
        self.0
            .retain(|p| p.input().responsibility != m.input().responsibility);
        self.0.push(m);
        Ok(())
    }
    fn invalidate(&mut self, _: ResponsibilityIdentity, _: RouteGeneration) -> bool {
        false
    }
    fn limits(&self) -> ManifestCacheLimits {
        ManifestCacheLimits {
            manifests: 16,
            bytes: MAX_CACHE_BYTES,
        }
    }
    fn usage(&self) -> ManifestCacheUsage {
        ManifestCacheUsage {
            manifests: self.0.len(),
            bytes: self.0.iter().map(|m| m.retained_bytes()).sum(),
        }
    }
}
fn hint_for(m: &ResponsibilityManifest, key: u8) -> RouteHint {
    resolve(
        &View(vec![m.clone()]),
        &Policy,
        m.input().responsibility,
        &[key],
        1,
    )
    .unwrap()
}
fn add(
    a: &mut RoutedApplication<BucketCounter<Policy>, Policy>,
    hint: RouteHint,
    id: u128,
    key: u8,
    delta: i64,
) {
    let b = encode_routed(
        hint,
        &[key],
        &encode_add(&[key], delta, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap();
    assert!(matches!(
        commit(a, id, b).outcome,
        RoutedOutcome::Applied(_)
    ));
}
#[test]
fn one_commit_moves_live_child_and_preserves_values_retries_and_parent_service() {
    let (plan, grants) = fixture();
    let mut d = initial(grants, 32);
    let mut a = owner(plan.child().clone(), 21, range(0, 128));
    let boot = a.bootstrap_command(200000).unwrap();
    commit(&mut a, 100, boot);
    let direct = hint_for(plan.child(), 1);
    add(&mut a, direct, 1, 1, 7);
    let mut retained = owner(plan.old_parent().clone(), 20, range(128, 256));
    let boot = retained.bootstrap_command(200000).unwrap();
    commit(&mut retained, 100, boot);
    add(&mut retained, hint_for(plan.old_parent(), 200), 1, 200, 11);
    let bytes = plan.encode(MAX_REPARENT_PLAN_BYTES).unwrap();
    let first = commit(&mut d, 200, bytes.clone());
    assert_eq!(first.outcome, DirectoryOutcome::Reparented);
    let updated = plan.updated_manifests();
    for m in &updated {
        assert_eq!(d.manifest(m.input().responsibility), Some(m));
        assert_eq!(m.input().epoch.get(), 1);
        assert_eq!(m.input().generation.get(), 2);
    }
    let fresh = View(
        d.plan()
            .manifests()
            .map(|m| d.manifest(m.input().responsibility).unwrap().clone())
            .collect(),
    );
    assert_eq!(
        resolve(
            &fresh,
            &Policy,
            plan.old_parent().input().responsibility,
            &[1],
            3
        ),
        Err(RoutingError::Vacant)
    );
    let routed = resolve(
        &fresh,
        &Policy,
        plan.new_parent().input().responsibility,
        &[1],
        3,
    )
    .unwrap();
    assert_eq!(routed.group, group(21));
    assert_eq!(
        a.read_at(
            a.applied_index(),
            RoutedQuery {
                hint: routed,
                key: vec![1],
                query: vec![1]
            }
        )
        .unwrap(),
        RoutedRead::Served(7)
    );
    // Same operation returns the original outcome; the parent pointer is absent
    // from ordinary data hints, so this is still the exact same physical owner.
    add(&mut a, routed, 1, 1, 7);
    assert_eq!(a.application().outbox().count(), 1);
    assert_eq!(
        retained
            .read_at(
                retained.applied_index(),
                RoutedQuery {
                    hint: hint_for(&updated[0], 200),
                    key: vec![200],
                    query: vec![200]
                }
            )
            .unwrap(),
        RoutedRead::Served(11)
    );
    let status = d
        .reparent_status_at(d.applied_index(), op(200))
        .unwrap()
        .unwrap();
    assert_eq!(status.index, first.index);
    assert_eq!(status.plan, plan);
    d = recover(&d);
    assert_eq!(
        d.reparent_status_at(d.applied_index(), op(200)).unwrap(),
        Some(status.clone())
    );
    assert!(commit(&mut d, 200, bytes.clone()).duplicate);
    assert_eq!(
        commit(&mut d, 201, bytes).outcome,
        DirectoryOutcome::GenerationMismatch
    );
    // Move the live subtree back using its actual current manifests.
    let back =
        ReparentPlan::new(updated[1].clone(), updated[0].clone(), updated[2].clone()).unwrap();
    assert_eq!(
        commit(&mut d, 202, back.encode(200000).unwrap()).outcome,
        DirectoryOutcome::Reparented
    );
    d = recover(&d);
    let view = LifecycleDirectory::new(d);
    let q = DirectoryQuery::Reparent(op(200));
    let result = view.read_at(view.applied_index(), q).unwrap();
    assert_eq!(result, DirectoryRead::Reparent(Some(status)));
    let nested = view.read_result_bytes(&result, usize::MAX).unwrap();
    assert!(nested > 0);
    assert_eq!(
        view.read_result_bound(&q).unwrap(),
        std::mem::size_of::<DirectoryRead>() + nested
    );
    assert!(view.read_result_bytes(&result, nested - 1).is_err());
    assert_eq!(
        view.read_at(view.applied_index() + 1, q),
        Err(ApplicationError::NotApplied)
    );
}
#[test]
fn nested_child_moves_without_changing_descendants_or_their_data() {
    let (base, mut ms) = fixture();
    let mut grand = base.child().clone().into_input();
    grand.responsibility.id = ResponsibilityId::new(12).unwrap();
    grand.parent = Some(ParentAuthority {
        responsibility: base.child().input().responsibility,
        group: group(1),
    });
    grand.scope = range(0, 64);
    grand.execution = ExecutionMode::Single(group(22));
    let grand = ResponsibilityManifest::new(grand).unwrap();
    let mut child = base.child().clone().into_input();
    child.execution = ExecutionMode::Delegated(vec![
        RouteEntry {
            scope: range(0, 64),
            target: RouteTarget::Child(ChildAuthority {
                responsibility: grand.input().responsibility,
                group: group(1),
                epoch: grand.input().epoch,
            }),
        },
        RouteEntry {
            scope: range(64, 128),
            target: RouteTarget::Group(group(21)),
        },
    ]);
    let child = ResponsibilityManifest::new(child).unwrap();
    ms[2] = child.clone();
    ms.push(grand.clone());
    let plan =
        ReparentPlan::new(base.old_parent().clone(), base.new_parent().clone(), child).unwrap();
    let mut d = initial(ms.clone(), 32);
    let mut a = owner(grand.clone(), 22, range(0, 64));
    let boot = a.bootstrap_command(200000).unwrap();
    commit(&mut a, 100, boot);
    add(&mut a, hint_for(&grand, 1), 1, 1, 13);
    assert_eq!(
        commit(&mut d, 200, plan.encode(200000).unwrap()).outcome,
        DirectoryOutcome::Reparented
    );
    let d = recover(&d);
    assert_eq!(d.manifest(grand.input().responsibility), Some(&grand));
    let fresh = View(
        ms.iter()
            .map(|m| d.manifest(m.input().responsibility).unwrap().clone())
            .collect(),
    );
    let hint = resolve(
        &fresh,
        &Policy,
        plan.new_parent().input().responsibility,
        &[1],
        3,
    )
    .unwrap();
    assert_eq!(hint.group, group(22));
    assert_eq!(
        a.read_at(
            a.applied_index(),
            RoutedQuery {
                hint,
                key: vec![1],
                query: vec![1]
            }
        )
        .unwrap(),
        RoutedRead::Served(13)
    );
}
#[test]
fn shape_and_live_destination_refuse_before_ownership_can_change() {
    let (plan, _) = fixture();
    let mut new = plan.new_parent().clone().into_input();
    let ExecutionMode::Delegated(r) = &mut new.execution else {
        unreachable!()
    };
    r[0].target = RouteTarget::Group(group(99));
    r[1].target = RouteTarget::Vacant;
    assert!(ReparentPlan::new(
        plan.old_parent().clone(),
        ResponsibilityManifest::new(new).unwrap(),
        plan.child().clone()
    )
    .is_err());
    for field in 0..5 {
        let mut child = plan.child().clone().into_input();
        match field {
            0 => child.authority = group(2),
            1 => child.parent = None,
            2 => child.scheme.version += 1,
            3 => child.scope = range(0, 64),
            _ => child.state = ResponsibilityState::Fenced,
        }
        assert!(ReparentPlan::new(
            plan.old_parent().clone(),
            plan.new_parent().clone(),
            ResponsibilityManifest::new(child).unwrap()
        )
        .is_err());
    }
    let bytes = plan.encode(200000).unwrap();
    assert_eq!(ReparentPlan::decode(&bytes).unwrap(), plan);
    for n in 0..bytes.len() {
        assert!(ReparentPlan::decode(&bytes[..n]).is_err());
    }
    assert!(plan.encode(bytes.len() - 1).is_err());
    let mut b = bytes;
    b.push(0);
    assert!(ReparentPlan::decode(&b).is_err());
}
#[test]
fn cycles_and_foreign_ancestry_or_subtrees_refuse_without_partial_updates() {
    let (base, mut ms) = fixture();
    let mut new = base.new_parent().clone().into_input();
    new.scope = range(0, 128);
    new.parent = Some(ParentAuthority {
        responsibility: base.child().input().responsibility,
        group: group(1),
    });
    new.execution = ExecutionMode::Delegated(vec![RouteEntry {
        scope: range(0, 128),
        target: RouteTarget::Vacant,
    }]);
    let new = ResponsibilityManifest::new(new).unwrap();
    let mut child = base.child().clone().into_input();
    child.execution = ExecutionMode::Delegated(vec![RouteEntry {
        scope: range(0, 128),
        target: RouteTarget::Child(ChildAuthority {
            responsibility: new.input().responsibility,
            group: group(1),
            epoch: new.input().epoch,
        }),
    }]);
    let child = ResponsibilityManifest::new(child).unwrap();
    let plan = ReparentPlan::new(base.old_parent().clone(), new.clone(), child.clone()).unwrap();
    let mut d = initial(vec![base.old_parent().clone(), child, new], 32);
    assert_eq!(
        commit(&mut d, 200, plan.encode(200000).unwrap()).outcome,
        DirectoryOutcome::ReparentRejected(RoutingError::Cycle)
    );
    for m in d.plan().manifests() {
        assert_eq!(d.manifest(m.input().responsibility), Some(m));
    }
    assert!(recover(&d)
        .reparent_status_at(d.applied_index(), op(200))
        .unwrap()
        .is_none());
    let mut new = base.new_parent().clone().into_input();
    new.parent = Some(ParentAuthority {
        responsibility: ResponsibilityIdentity {
            id: ResponsibilityId::new(90).unwrap(),
            incarnation: ResponsibilityIncarnation::new(1).unwrap(),
        },
        group: group(9),
    });
    let new = ResponsibilityManifest::new(new).unwrap();
    ms[1] = new.clone();
    let plan = ReparentPlan::new(base.old_parent().clone(), new, base.child().clone()).unwrap();
    let mut d = initial(ms, 32);
    assert_eq!(
        commit(&mut d, 200, plan.encode(200000).unwrap()).outcome,
        DirectoryOutcome::ReparentRejected(RoutingError::WrongParent)
    );
    let (_, mut ms) = fixture();
    let mut child = base.child().clone().into_input();
    child.execution = ExecutionMode::Delegated(vec![RouteEntry {
        scope: range(0, 128),
        target: RouteTarget::Child(ChildAuthority {
            responsibility: ResponsibilityIdentity {
                id: ResponsibilityId::new(90).unwrap(),
                incarnation: ResponsibilityIncarnation::new(1).unwrap(),
            },
            group: group(9),
            epoch: OwnershipEpoch::new(1).unwrap(),
        }),
    }]);
    let child = ResponsibilityManifest::new(child).unwrap();
    ms[2] = child.clone();
    let plan =
        ReparentPlan::new(base.old_parent().clone(), base.new_parent().clone(), child).unwrap();
    let mut d = initial(ms, 32);
    assert_eq!(
        commit(&mut d, 200, plan.encode(200000).unwrap()).outcome,
        DirectoryOutcome::ReparentRejected(RoutingError::WrongChild)
    );
}
#[test]
fn destination_depth_is_checked_against_the_whole_moved_subtree() {
    let (base, _) = fixture();
    let rid = |n| ResponsibilityIdentity {
        id: ResponsibilityId::new(n).unwrap(),
        incarnation: ResponsibilityIncarnation::new(1).unwrap(),
    };
    let mut chain = Vec::new();
    for n in 0..MAX_ROUTE_HOPS {
        let mut m = base.new_parent().clone().into_input();
        m.responsibility = rid(50 + n as u128);
        m.scope = range(0, 128);
        m.parent = (n > 0).then(|| ParentAuthority {
            responsibility: rid(49 + n as u128),
            group: group(1),
        });
        m.execution = ExecutionMode::Delegated(vec![RouteEntry {
            scope: m.scope,
            target: if n + 1 == MAX_ROUTE_HOPS {
                RouteTarget::Vacant
            } else {
                RouteTarget::Child(ChildAuthority {
                    responsibility: rid(51 + n as u128),
                    group: group(1),
                    epoch: m.epoch,
                })
            },
        }]);
        chain.push(ResponsibilityManifest::new(m).unwrap());
    }
    let plan = ReparentPlan::new(
        base.old_parent().clone(),
        chain.last().unwrap().clone(),
        base.child().clone(),
    )
    .unwrap();
    let mut grants = vec![base.old_parent().clone(), base.child().clone()];
    grants.extend(chain);
    let mut d = initial(grants, 128);
    assert_eq!(
        commit(&mut d, 200, plan.encode(200000).unwrap()).outcome,
        DirectoryOutcome::ReparentRejected(RoutingError::HopLimit)
    );
    assert_eq!(
        d.manifest(base.child().input().responsibility),
        Some(base.child())
    );
}
#[test]
fn lifecycle_reservations_capacity_and_batch_failure_cannot_half_move_a_child() {
    let (plan, grants) = fixture();
    let bytes = plan.encode(200000).unwrap();
    for target in [plan.old_parent(), plan.new_parent(), plan.child()] {
        let mut d = initial(grants.clone(), 32);
        assert_eq!(
            commit(
                &mut d,
                210,
                DeletionIntent {
                    before: target.clone()
                }
                .encode(200000)
                .unwrap()
            )
            .outcome,
            DirectoryOutcome::DeletionIntentRecorded
        );
        assert_eq!(
            commit(&mut d, 200, bytes.clone()).outcome,
            DirectoryOutcome::LifecycleBusy
        );
        assert_eq!(
            d.manifest(plan.child().input().responsibility),
            Some(plan.child())
        );
    }
    let mut after = plan.child().clone().into_input();
    after.generation = RouteGeneration::new(2).unwrap();
    after.epoch = OwnershipEpoch::new(2).unwrap();
    after.execution = ExecutionMode::Partitioned(vec![
        RouteEntry {
            scope: range(0, 64),
            target: RouteTarget::Group(group(31)),
        },
        RouteEntry {
            scope: range(64, 128),
            target: RouteTarget::Group(group(32)),
        },
    ]);
    let delegation = voteboat::delegation::DelegationPlan::new(
        plan.old_parent().clone(),
        plan.child().clone(),
        ResponsibilityManifest::new(after).unwrap(),
        op(500),
    )
    .unwrap();
    let mut pending = initial(grants.clone(), 32);
    assert_eq!(
        commit(&mut pending, 400, delegation.encode(200000).unwrap()).outcome,
        DirectoryOutcome::DelegationReserved
    );
    assert_eq!(
        commit(&mut pending, 200, bytes.clone()).outcome,
        DirectoryOutcome::LifecycleBusy
    );
    let mut d = initial(grants.clone(), 32);
    let create = GroupCreationIntent {
        authority: group(1),
        parent: plan.child().input().responsibility,
        expected: plan.child().input().generation,
        responsibility: ResponsibilityIdentity {
            id: ResponsibilityId::new(90).unwrap(),
            incarnation: ResponsibilityIncarnation::new(1).unwrap(),
        },
        bootstrap: support::bootstrap(90, 3),
        application: plan.child().input().application,
        mode: GroupCreationMode::Staging,
    };
    assert_eq!(
        commit(&mut d, 210, create.encode(200000).unwrap()).outcome,
        DirectoryOutcome::CreationReserved
    );
    assert_eq!(
        commit(&mut d, 200, bytes.clone()).outcome,
        DirectoryOutcome::LifecycleBusy
    );
    let mut d = initial(grants.clone(), 4);
    assert_eq!(d.remaining_operations(), 0);
    assert!(d
        .validate_proposal(op(200), &bytes, std::iter::empty())
        .is_err());
    let index = d.applied_index();
    assert_eq!(
        d.apply_batch(&[entry(index + 1, 200, bytes.clone())]),
        Err(ApplicationError::DedupCapacity)
    );
    assert_eq!(d.applied_index(), index);
    let mut d = initial(grants, 32);
    let index = d.applied_index();
    assert!(d
        .apply_batch(&[
            entry(index + 1, 200, bytes.clone()),
            entry(index + 2, 999, vec![0])
        ])
        .is_err());
    assert_eq!(d.applied_index(), index);
    for m in [plan.old_parent(), plan.new_parent(), plan.child()] {
        assert_eq!(d.manifest(m.input().responsibility), Some(m));
    }
    assert_eq!(
        commit(&mut d, 200, bytes).outcome,
        DirectoryOutcome::Reparented
    );
    let changed = ReparentPlan::new(
        plan.updated_manifests()[1].clone(),
        plan.updated_manifests()[0].clone(),
        plan.updated_manifests()[2].clone(),
    )
    .unwrap();
    assert_eq!(
        commit(&mut d, 200, changed.encode(200000).unwrap()).outcome,
        DirectoryOutcome::OperationConflict
    );
}
#[cfg(feature = "native")]
#[test]
fn native_cache_rebinds_parent_and_both_routes_without_moving_data_ownership() {
    use voteboat::native::routing::NativeManifestCache;
    let (plan, grants) = fixture();
    let mut default_cache = NativeManifestCache::new(ManifestCacheLimits {
        manifests: 16,
        bytes: MAX_CACHE_BYTES,
    })
    .unwrap();
    for m in grants.clone() {
        default_cache.admit(m).unwrap();
    }
    let defaults = plan.updated_manifests();
    assert_eq!(
        default_cache.admit(defaults[1].clone()).unwrap_err().0,
        RoutingError::EpochMismatch
    );
    assert_eq!(
        default_cache.admit(defaults[2].clone()).unwrap_err().0,
        RoutingError::IdentityChange
    );
    assert!(default_cache.with_local_reparenting().is_err());
    let mut cache = NativeManifestCache::new(ManifestCacheLimits {
        manifests: 16,
        bytes: MAX_CACHE_BYTES,
    })
    .unwrap()
    .with_local_reparenting()
    .unwrap_or_else(|_| panic!("selected reparent cache"));
    for m in grants {
        cache.admit(m).unwrap();
    }
    let updates = plan.updated_manifests();
    cache.admit(updates[1].clone()).unwrap();
    assert_eq!(
        resolve(
            &cache,
            &Policy,
            plan.new_parent().input().responsibility,
            &[1],
            3
        ),
        Err(RoutingError::WrongParent)
    );
    cache.admit(updates[2].clone()).unwrap();
    cache.admit(updates[0].clone()).unwrap();
    let h = resolve(
        &cache,
        &Policy,
        plan.new_parent().input().responsibility,
        &[1],
        3,
    )
    .unwrap();
    assert_eq!(h.group, group(21));
    assert_eq!(
        resolve(
            &cache,
            &Policy,
            plan.old_parent().input().responsibility,
            &[1],
            3
        ),
        Err(RoutingError::Vacant)
    );
    for m in [plan.old_parent(), plan.new_parent(), plan.child()] {
        assert_eq!(
            cache.admit(m.clone()).unwrap_err().0,
            RoutingError::StaleGeneration
        );
    }
    let mut changed = updates[2].clone().into_input();
    changed.generation = RouteGeneration::new(3).unwrap();
    changed.authority = group(9);
    assert_eq!(
        cache
            .admit(ResponsibilityManifest::new(changed).unwrap())
            .unwrap_err()
            .0,
        RoutingError::IdentityChange
    );
}
#[test]
fn original_profiles_refuse_reparenting_and_checkpoint_restore_is_atomic() {
    let (plan, grants) = fixture();
    let bytes = plan.encode(200000).unwrap();
    let mut d = initial(grants.clone(), 32);
    commit(&mut d, 200, bytes.clone());
    let snapshot = d.checkpoint(200000).unwrap();
    let mut old = Directory::new(d.plan().clone(), d.limits())
        .unwrap()
        .with_child_slot_retirement()
        .unwrap_or_else(|_| panic!());
    let boot = old.bootstrap_command(200000).unwrap();
    commit(&mut old, 1000, boot);
    assert!(old
        .validate_proposal(op(200), &bytes, std::iter::empty())
        .is_err());
    assert!(old
        .restore_checkpoint(10, d.applied_index(), &snapshot)
        .is_err());
    let pristine = fresh(grants, 32).checkpoint(200000).unwrap();
    let mut receiver = fresh(d.plan().manifests().cloned().collect(), 32);
    for n in 0..snapshot.len() {
        assert!(receiver
            .restore_checkpoint(11, d.applied_index(), &snapshot[..n])
            .is_err());
        assert_eq!(receiver.checkpoint(200000).unwrap(), pristine);
    }
    assert!(d.with_local_reparenting().is_err());
}
#[cfg(feature = "native")]
#[test]
fn journal_cuts_never_publish_only_one_parent_or_lose_the_original_result() {
    use support::{Fault, ModelIo};
    use voteboat::{log::*, native::log_store::*};
    let (plan, grants) = fixture();
    let d = initial(grants.clone(), 32);
    let command = plan.encode(200000).unwrap();
    let mut prefix = vec![entry(1, 1000, d.bootstrap_command(200000).unwrap())];
    for (i, m) in grants.iter().enumerate() {
        prefix.push(entry(
            2 + i as u64,
            1001 + i as u128,
            DirectoryCommand {
                expected: None,
                manifest: m.clone(),
            }
            .encode(200000)
            .unwrap(),
        ));
    }
    let limits = LogLimits::default();
    let seed = || {
        let io = ModelIo::default();
        let mut log = NativeLogStore::create(io.clone(), support::identity(1), limits).unwrap();
        support::append(
            &mut log,
            vec![LogMutation::Create(support::bootstrap(1, 3))],
        );
        let state = log.state(group(1)).unwrap();
        support::append(
            &mut log,
            vec![support::update(
                &state,
                1,
                4,
                Some(Suffix {
                    from: 1,
                    entries: prefix.clone(),
                }),
            )],
        );
        (io, log)
    };
    let (_, log) = seed();
    let mutation = support::update(
        &log.state(group(1)).unwrap(),
        1,
        5,
        Some(Suffix {
            from: 5,
            entries: vec![entry(5, 200, command.clone())],
        }),
    );
    let frame = NativeLogCodec
        .encode_batch(3, std::slice::from_ref(&mutation), limits)
        .unwrap();
    let (mut old, mut complete) = (false, false);
    for fault in (0..=frame.len()).map(Fault::Append).chain([
        Fault::Sync,
        Fault::PublishBefore,
        Fault::PublishAfter,
    ]) {
        let (io, mut log) = seed();
        io.0.borrow_mut().fault = fault;
        if let Ok(tickets) = log.append_batch(vec![mutation.clone()]) {
            assert!(log.barrier(&tickets).is_err());
        }
        drop(log);
        io.0.borrow_mut().power_loss();
        let log = NativeLogStore::recover(io, support::identity(1), limits).unwrap();
        let state = log.state(group(1)).unwrap();
        let mut app = fresh(grants.clone(), 32);
        app.apply_batch(
            &state
                .entries
                .into_iter()
                .filter(|e| e.index <= state.commit_index)
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let done = state.commit_index == 5;
        if done {
            complete = true
        } else {
            old = true;
            assert_eq!(state.commit_index, 4)
        }
        let expected = if done {
            plan.updated_manifests().to_vec()
        } else {
            grants.clone()
        };
        for m in expected {
            assert_eq!(app.manifest(m.input().responsibility), Some(&m));
        }
        assert_eq!(
            app.reparent_status_at(app.applied_index(), op(200))
                .unwrap()
                .is_some(),
            done
        );
        app = recover(&app);
        let receipt = commit(&mut app, 200, command.clone());
        assert_eq!(receipt.duplicate, done);
        assert_eq!(receipt.outcome, DirectoryOutcome::Reparented);
        assert_eq!(
            app.reparent_status_at(app.applied_index(), op(200))
                .unwrap()
                .unwrap()
                .index,
            5
        );
    }
    assert!(old && complete);
}
