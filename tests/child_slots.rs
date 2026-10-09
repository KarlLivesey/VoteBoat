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
    application::*, bucket_counter::*, child_slots::*, deletion::*, directory::*, identity::*,
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
    .with_child_slot_retirement()
    .unwrap_or_else(|_| panic!("schema9"))
}
fn initial(ms: Vec<ResponsibilityManifest>, operations: usize) -> Directory {
    let mut d = fresh(ms.clone(), operations);
    let b = d.bootstrap_command(200000).unwrap();
    commit(&mut d, 1000, b);
    for (i, m) in ms.into_iter().enumerate() {
        commit(
            &mut d,
            1001 + i as u128,
            DirectoryCommand {
                expected: None,
                manifest: m,
            }
            .encode(200000)
            .unwrap(),
        );
    }
    d
}
fn recover(d: &Directory) -> Directory {
    let mut next = fresh(
        d.plan().manifests().cloned().collect(),
        d.limits().operations,
    );
    next.restore_checkpoint(10, d.applied_index(), &d.checkpoint(2000000).unwrap())
        .unwrap();
    next
}
fn reserve(d: &mut Directory, id: u128, m: &ResponsibilityManifest) -> DeletionIntentStatus {
    let r = commit(
        d,
        id,
        DeletionIntent { before: m.clone() }.encode(200000).unwrap(),
    );
    assert_eq!(r.outcome, DirectoryOutcome::DeletionIntentRecorded);
    d.deletion_intent_at(d.applied_index(), op(id))
        .unwrap()
        .unwrap()
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
fn fence(
    a: &mut RoutedApplication<BucketCounter<Policy>, Policy>,
    intent: u128,
) -> DeletionFenceEvidence {
    let boot = a.bootstrap_command(200000).unwrap();
    commit(a, 100, boot);
    let b = encode_fence(a.grant().input().epoch);
    let r = commit(a, intent, b);
    let RoutedOutcome::Fenced(fence) = r.outcome else {
        panic!("fence")
    };
    DeletionFenceEvidence {
        configuration: ConfigurationId::new(3).unwrap(),
        fence,
    }
}
fn finish(d: &mut Directory, id: u128, c: &DeletionCompletion) -> DeletionStatus {
    let r = commit(d, id, c.encode(200000).unwrap());
    assert_eq!(
        r.outcome,
        DirectoryOutcome::Deleted(
            RouteGeneration::new(c.intent.intent.before.input().generation.get() + 1).unwrap()
        )
    );
    d.deletion_status_at(d.applied_index(), c.intent.operation)
        .unwrap()
        .unwrap()
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
fn deleted_child(
    d: &mut Directory,
    child: &ResponsibilityManifest,
) -> (
    ChildDeletionEvidence,
    RoutedApplication<BucketCounter<Policy>, Policy>,
) {
    let intent = reserve(d, 210, child);
    let ExecutionMode::Single(group) = child.input().execution else {
        panic!("leaf fixture")
    };
    let mut a = owner(child.clone(), group.id.get(), child.input().scope);
    let f = fence(&mut a, 210);
    let status = finish(
        d,
        211,
        &DeletionCompletion {
            intent,
            fences: vec![f],
            children: vec![],
        },
    );
    (
        ChildDeletionEvidence::from_status(ConfigurationId::new(3).unwrap(), &status).unwrap(),
        a,
    )
}
// An immutable host view exercises the public resolver even without native features.
struct View(Vec<ResponsibilityManifest>);
impl ManifestCache for View {
    fn get(&self, id: ResponsibilityIdentity) -> Option<&ResponsibilityManifest> {
        self.0.iter().find(|m| m.input().responsibility == id)
    }
    fn admit(
        &mut self,
        m: ResponsibilityManifest,
    ) -> Result<(), (RoutingError, ResponsibilityManifest)> {
        Err((RoutingError::Capacity, m))
    }
    fn invalidate(&mut self, _: ResponsibilityIdentity, _: RouteGeneration) -> bool {
        false
    }
    fn limits(&self) -> ManifestCacheLimits {
        ManifestCacheLimits {
            manifests: 8,
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
fn retirement(foreign: bool) {
    let (parent, child) = tree(foreign);
    let mut pd = initial(
        if foreign {
            vec![parent.clone()]
        } else {
            vec![parent.clone(), child.clone()]
        },
        32,
    );
    let mut cd = if foreign {
        initial(vec![child.clone()], 16)
    } else {
        pd.clone()
    };
    let stale = resolve(
        &View(vec![parent.clone(), child.clone()]),
        &Policy,
        parent.input().responsibility,
        &[1],
        2,
    )
    .unwrap();
    let (fact, ca) = deleted_child(&mut cd, &child);
    if !foreign {
        pd = cd.clone();
    }
    let r = RetireChildSlot {
        before: parent.clone(),
        child: fact,
    };
    let bytes = r.encode(MAX_RETIRE_CHILD_SLOT_BYTES).unwrap();
    assert_eq!(RetireChildSlot::decode(&bytes).unwrap(), r);
    let after = r.after().unwrap();
    assert!(after.retires_child_slots(&parent));
    assert!(!parent.retires_child_slots(&after));
    assert!(!after.refreshes_child_epochs(&parent));
    let receipt = commit(&mut pd, 220, bytes.clone());
    assert_eq!(
        receipt.outcome,
        DirectoryOutcome::ChildSlotRetired(RouteGeneration::new(2).unwrap())
    );
    assert_eq!(pd.manifest(parent.input().responsibility), Some(&after));
    assert_eq!(after.input().epoch, parent.input().epoch);
    assert_eq!(
        resolve(
            &View(vec![after.clone()]),
            &Policy,
            parent.input().responsibility,
            &[1],
            2
        ),
        Err(RoutingError::Vacant)
    );
    assert_eq!(ca.check_context(&stale, &[1]), Err(RoutingError::Fenced));
    // Fresh and previously cached parent hints still address the original retained owner.
    let old_hint = resolve(
        &View(vec![parent.clone()]),
        &Policy,
        parent.input().responsibility,
        &[200],
        1,
    )
    .unwrap();
    let new_hint = resolve(
        &View(vec![after.clone()]),
        &Policy,
        parent.input().responsibility,
        &[200],
        1,
    )
    .unwrap();
    let mut pa = owner(parent.clone(), 20, range(128, 256));
    let boot = pa.bootstrap_command(200000).unwrap();
    commit(&mut pa, 100, boot);
    let b = encode_routed(
        old_hint,
        &[200],
        &encode_add(&[200], 11, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap();
    assert!(matches!(
        commit(&mut pa, 1, b).outcome,
        RoutedOutcome::Applied(_)
    ));
    assert_eq!(
        pa.read_at(
            pa.applied_index(),
            RoutedQuery {
                hint: new_hint,
                key: vec![200],
                query: vec![200]
            }
        )
        .unwrap(),
        RoutedRead::Served(11)
    );
    let status = pd
        .retired_child_slot_at(pd.applied_index(), op(220))
        .unwrap()
        .unwrap();
    assert_eq!(status.retirement, r);
    assert_eq!(status.index, receipt.index);
    pd = recover(&pd);
    assert_eq!(
        pd.retired_child_slot_at(pd.applied_index(), op(220))
            .unwrap(),
        Some(status.clone())
    );
    assert!(commit(&mut pd, 220, bytes.clone()).duplicate);
    assert_eq!(
        commit(&mut pd, 221, bytes).outcome,
        DirectoryOutcome::GenerationMismatch
    );
    let view = LifecycleDirectory::new(pd);
    let query = DirectoryQuery::RetiredChildSlot(op(220));
    let result = view.read_at(view.applied_index(), query).unwrap();
    assert_eq!(result, DirectoryRead::RetiredChildSlot(Some(status)));
    let bound = view.read_result_bound(&query).unwrap();
    let nested = view.read_result_bytes(&result, usize::MAX).unwrap();
    assert_eq!(bound, std::mem::size_of::<DirectoryRead>() + nested);
    assert!(nested > 0);
    assert!(view.read_result_bytes(&result, nested - 1).is_err());
    assert_eq!(
        view.read_at(view.applied_index() + 1, query),
        Err(ApplicationError::NotApplied)
    );
}
#[test]
fn local_deleted_child_retirement_preserves_retained_service_and_replay() {
    retirement(false);
}
#[test]
fn foreign_deleted_child_retirement_preserves_retained_service_and_replay() {
    retirement(true);
}
#[test]
fn live_local_child_and_mismatched_original_facts_cannot_retire_a_slot() {
    let (parent, child) = tree(false);
    let mut pd = initial(vec![parent.clone(), child.clone()], 32);
    let mut cd = pd.clone();
    let (fact, _) = deleted_child(&mut cd, &child);
    let r = RetireChildSlot {
        before: parent.clone(),
        child: fact,
    };
    assert_eq!(
        commit(&mut pd, 220, r.encode(200000).unwrap()).outcome,
        DirectoryOutcome::TransferEvidenceMismatch
    );
    assert_eq!(pd.manifest(parent.input().responsibility), Some(&parent));
    assert!(pd
        .retired_child_slot_at(pd.applied_index(), op(220))
        .unwrap()
        .is_none());
    // The same fact is accepted only where its original deletion is present.
    assert!(matches!(
        commit(&mut cd, 220, r.encode(200000).unwrap()).outcome,
        DirectoryOutcome::ChildSlotRetired(_)
    ));
    for field in 0..6 {
        let mut wrong = r.clone();
        match field {
            0 => wrong.child.scope = range(0, 127),
            1 => wrong.child.epoch = OwnershipEpoch::new(2).unwrap(),
            2 => wrong.child.authority = group(99),
            3 => wrong.child.parent.group = group(99),
            4 => wrong.child.parent.responsibility.id = ResponsibilityId::new(99).unwrap(),
            _ => wrong.child.responsibility.id = ResponsibilityId::new(99).unwrap(),
        }
        assert!(wrong.encode(200000).is_err());
    }
    let mut wrong = r.clone();
    wrong.child.index += 1;
    assert_eq!(
        commit(&mut pd, 222, wrong.encode(200000).unwrap()).outcome,
        DirectoryOutcome::TransferEvidenceMismatch
    );
    // Neither a different valid request under the same operation nor a failed batch changes it.
    let before = cd.manifest(parent.input().responsibility).cloned();
    assert_eq!(
        commit(&mut cd, 220, wrong.encode(200000).unwrap()).outcome,
        DirectoryOutcome::OperationConflict
    );
    assert_eq!(cd.manifest(parent.input().responsibility).cloned(), before);
    let mut d = initial(vec![parent.clone(), child.clone()], 32);
    deleted_child(&mut d, &child);
    let index = d.applied_index();
    assert!(d
        .apply_batch(&[
            entry(index + 1, 220, r.encode(200000).unwrap()),
            entry(index + 2, 999, vec![0])
        ])
        .is_err());
    assert_eq!(d.applied_index(), index);
    assert_eq!(d.manifest(parent.input().responsibility), Some(&parent));
}
#[test]
fn deleting_an_all_vacant_parent_needs_no_fictional_owner_or_child_obligations() {
    let (parent, child) = tree(true);
    let mut m = parent.into_input();
    m.scope = range(0, 128);
    let ExecutionMode::Delegated(routes) = &mut m.execution else {
        unreachable!()
    };
    routes.pop();
    let parent = ResponsibilityManifest::new(m).unwrap();
    let mut pd = initial(vec![parent.clone()], 16);
    let mut cd = initial(vec![child.clone()], 16);
    let (fact, _) = deleted_child(&mut cd, &child);
    let r = RetireChildSlot {
        before: parent.clone(),
        child: fact,
    };
    commit(&mut pd, 220, r.encode(200000).unwrap());
    let empty = r.after().unwrap();
    assert!(empty.has_vacancies());
    let di = reserve(&mut pd, 230, &empty);
    let ds = finish(
        &mut pd,
        231,
        &DeletionCompletion {
            intent: di,
            fences: vec![],
            children: vec![],
        },
    );
    let pd = recover(&pd);
    assert_eq!(
        pd.deletion_status_at(pd.applied_index(), op(230)).unwrap(),
        Some(ds)
    );
    assert!(pd
        .retired_child_slot_at(pd.applied_index(), op(220))
        .unwrap()
        .is_some());
    assert!(cd
        .deletion_status_at(cd.applied_index(), op(210))
        .unwrap()
        .is_some());
}
#[test]
fn retirement_serializes_with_parent_lifecycle_and_uses_bounded_ordinary_capacity() {
    let (parent, child) = tree(true);
    let mut cd = initial(vec![child.clone()], 16);
    let (fact, _) = deleted_child(&mut cd, &child);
    let r = RetireChildSlot {
        before: parent.clone(),
        child: fact,
    };
    let bytes = r.encode(200000).unwrap();
    let mut pd = initial(vec![parent.clone()], 16);
    reserve(&mut pd, 200, &parent);
    assert_eq!(
        commit(&mut pd, 220, bytes.clone()).outcome,
        DirectoryOutcome::LifecycleBusy
    );
    let mut next = child.into_input();
    next.epoch = OwnershipEpoch::new(2).unwrap();
    next.generation = RouteGeneration::new(2).unwrap();
    next.execution = ExecutionMode::Partitioned(vec![
        RouteEntry {
            scope: range(0, 64),
            target: RouteTarget::Group(group(30)),
        },
        RouteEntry {
            scope: range(64, 128),
            target: RouteTarget::Group(group(31)),
        },
    ]);
    let plan = voteboat::delegation::DelegationPlan::new(
        parent.clone(),
        tree(true).1,
        ResponsibilityManifest::new(next).unwrap(),
        op(500),
    )
    .unwrap();
    let mut pd = initial(vec![parent.clone()], 16);
    commit(&mut pd, 400, plan.encode(200000).unwrap());
    assert_eq!(
        commit(&mut pd, 220, bytes.clone()).outcome,
        DirectoryOutcome::LifecycleBusy
    );
    let mut pd = initial(vec![parent.clone()], 2);
    assert_eq!(pd.remaining_operations(), 0);
    assert!(pd
        .validate_proposal(op(220), &bytes, std::iter::empty())
        .is_err());
    assert_eq!(
        pd.apply_batch(&[entry(pd.applied_index() + 1, 220, bytes.clone())]),
        Err(ApplicationError::DedupCapacity)
    );
    assert_eq!(pd.manifest(parent.input().responsibility), Some(&parent));
    let mut pd = initial(vec![parent], 3);
    assert!(pd
        .validate_proposal(op(220), &bytes, [(op(220), bytes.as_slice())].into_iter())
        .is_ok());
    assert!(pd
        .validate_proposal(op(221), &bytes, [(op(220), bytes.as_slice())].into_iter())
        .is_err());
    commit(&mut pd, 220, bytes.clone());
    assert!(commit(&mut pd, 220, bytes).duplicate);
    assert_eq!(pd.remaining_operations(), 0);
    assert_eq!(pd.reserved_publication_bytes(), 0);
    assert_eq!(recover(&pd).remaining_operations(), 0);
}
#[test]
fn vacancy_codecs_and_original_directory_profiles_fail_closed() {
    let (parent, child) = tree(true);
    let mut cd = initial(vec![child.clone()], 16);
    let (fact, _) = deleted_child(&mut cd, &child);
    let r = RetireChildSlot {
        before: parent.clone(),
        child: fact,
    };
    let bytes = r.encode(200000).unwrap();
    for n in 0..bytes.len() {
        assert!(RetireChildSlot::decode(&bytes[..n]).is_err());
    }
    assert!(r.encode(bytes.len() - 1).is_err());
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(RetireChildSlot::decode(&trailing).is_err());
    let after = r.after().unwrap();
    let command = DirectoryCommand {
        expected: Some(parent.input().generation),
        manifest: after.clone(),
    }
    .encode(200000)
    .unwrap();
    let offset = command.windows(8).position(|s| s == b"VBMAN002").unwrap();
    let mut downgrade = command.clone();
    downgrade[offset..offset + 8].copy_from_slice(b"VBMAN001");
    assert!(DirectoryCommand::decode(&downgrade).is_err());
    let original = DirectoryCommand {
        expected: None,
        manifest: parent.clone(),
    }
    .encode(200000)
    .unwrap();
    let offset = original.windows(8).position(|s| s == b"VBMAN001").unwrap();
    let mut fake = original.clone();
    fake[offset..offset + 8].copy_from_slice(b"VBMAN002");
    assert!(DirectoryCommand::decode(&fake).is_err());
    assert_eq!(
        DirectoryCommand::decode(&original).unwrap().manifest,
        parent
    );
    let mut partitioned = after.clone().into_input();
    let ExecutionMode::Delegated(routes) = partitioned.execution else {
        unreachable!()
    };
    partitioned.execution = ExecutionMode::Partitioned(routes);
    assert!(ResponsibilityManifest::new(partitioned).is_err());
    let mut pd = initial(vec![parent.clone()], 16);
    commit(&mut pd, 220, bytes.clone());
    let snapshot = pd.checkpoint(2000000).unwrap();
    for profile in 1..=9 {
        let select = |ms| {
            let d = Directory::new(DirectoryPlan::new(group(1), ms).unwrap(), pd.limits()).unwrap();
            match profile {
                1 => d,
                2 => d.with_group_creation().unwrap_or_else(|_| panic!()),
                3 => d.with_namespace_creation().unwrap_or_else(|_| panic!()),
                4 => d.with_namespace_transfers().unwrap_or_else(|_| panic!()),
                5 => d
                    .with_responsibility_insertion()
                    .unwrap_or_else(|_| panic!()),
                6 => d.with_recursive_insertion().unwrap_or_else(|_| panic!()),
                7 => d
                    .with_cross_authority_insertion()
                    .unwrap_or_else(|_| panic!()),
                8 => d.with_retained_insertion().unwrap_or_else(|_| panic!()),
                _ => d.with_namespace_deletion().unwrap_or_else(|_| panic!()),
            }
        };
        let mut old = select(vec![parent.clone()]);
        let boot = old.bootstrap_command(200000).unwrap();
        commit(&mut old, 1000, boot);
        for b in [&bytes, &command] {
            assert!(old
                .validate_proposal(op(220), b, std::iter::empty())
                .is_err());
        }
        assert!(old
            .restore_checkpoint(profile, pd.applied_index(), &snapshot)
            .is_err());
        let mut empty_grant = after.clone().into_input();
        empty_grant.generation = RouteGeneration::new(1).unwrap();
        assert!(
            select(vec![ResponsibilityManifest::new(empty_grant).unwrap()])
                .bootstrap_command(200000)
                .is_err()
        );
    }
    assert!(pd.clone().with_child_slot_retirement().is_err());
}
#[cfg(feature = "native")]
#[test]
fn native_cache_accepts_only_monotonic_trusted_child_retirement() {
    use voteboat::native::routing::NativeManifestCache;
    let (parent, child) = tree(true);
    let mut cd = initial(vec![child.clone()], 16);
    let (fact, _) = deleted_child(&mut cd, &child);
    let after = RetireChildSlot {
        before: parent.clone(),
        child: fact,
    }
    .after()
    .unwrap();
    let mut cache = NativeManifestCache::new(ManifestCacheLimits {
        manifests: 8,
        bytes: MAX_CACHE_BYTES,
    })
    .unwrap();
    cache.admit(parent.clone()).unwrap();
    cache.admit(after.clone()).unwrap();
    assert_eq!(
        resolve(&cache, &Policy, parent.input().responsibility, &[1], 2),
        Err(RoutingError::Vacant)
    );
    assert_eq!(
        cache.admit(parent.clone()).unwrap_err().0,
        RoutingError::StaleGeneration
    );
    let mut resurrect = parent.clone().into_input();
    resurrect.generation = RouteGeneration::new(3).unwrap();
    assert_eq!(
        cache
            .admit(ResponsibilityManifest::new(resurrect).unwrap())
            .unwrap_err()
            .0,
        RoutingError::EpochMismatch
    );
    let mut erase_owner = after.clone().into_input();
    erase_owner.generation = RouteGeneration::new(3).unwrap();
    let ExecutionMode::Delegated(r) = &mut erase_owner.execution else {
        unreachable!()
    };
    r[1].target = RouteTarget::Vacant;
    assert_eq!(
        cache
            .admit(ResponsibilityManifest::new(erase_owner).unwrap())
            .unwrap_err()
            .0,
        RoutingError::EpochMismatch
    );
    assert_eq!(cache.get(parent.input().responsibility), Some(&after));
    assert_eq!(cache.usage().bytes, after.retained_bytes());
}
#[test]
fn retirement_preserves_pending_creation_publication_path() {
    let (parent, child) = tree(true);
    let mut cd = initial(vec![child.clone()], 16);
    let (fact, _) = deleted_child(&mut cd, &child);
    let r = RetireChildSlot {
        before: parent.clone(),
        child: fact,
    };
    let mut pd = initial(vec![parent.clone()], 16);
    let creation = GroupCreationIntent {
        authority: group(1),
        parent: parent.input().responsibility,
        expected: parent.input().generation,
        responsibility: ResponsibilityIdentity {
            id: ResponsibilityId::new(90).unwrap(),
            incarnation: ResponsibilityIncarnation::new(1).unwrap(),
        },
        bootstrap: support::bootstrap(90, 3),
        application: parent.input().application,
        mode: GroupCreationMode::Staging,
    };
    assert_eq!(
        commit(&mut pd, 90, creation.encode(200000).unwrap()).outcome,
        DirectoryOutcome::CreationReserved
    );
    assert_eq!(
        commit(&mut pd, 220, r.encode(200000).unwrap()).outcome,
        DirectoryOutcome::LifecycleBusy
    );
    let pd = recover(&pd);
    assert_eq!(pd.manifest(parent.input().responsibility), Some(&parent));
    assert!(pd
        .group_creation_at(pd.applied_index(), group(90))
        .unwrap()
        .is_some());
}
#[test]
fn successive_slot_retirements_replay_vacant_manifests_and_keep_identities_reserved() {
    let (parent, left) = tree(true);
    let mut right = left.clone().into_input();
    right.responsibility.id = ResponsibilityId::new(12).unwrap();
    right.authority = group(3);
    right.scope = range(128, 256);
    right.execution = ExecutionMode::Single(group(22));
    let right = ResponsibilityManifest::new(right).unwrap();
    let mut parent = parent.into_input();
    let ExecutionMode::Delegated(routes) = &mut parent.execution else {
        unreachable!()
    };
    routes[1].target = RouteTarget::Child(ChildAuthority {
        responsibility: right.input().responsibility,
        group: right.input().authority,
        epoch: right.input().epoch,
    });
    let parent = ResponsibilityManifest::new(parent).unwrap();
    let mut pd = initial(vec![parent.clone()], 16);
    let mut before = parent;
    for (i, child) in [left.clone(), right].into_iter().enumerate() {
        let mut cd = initial(vec![child.clone()], 16);
        let (fact, _) = deleted_child(&mut cd, &child);
        let r = RetireChildSlot {
            before,
            child: fact,
        };
        let bytes = r.encode(200000).unwrap();
        assert_eq!(RetireChildSlot::decode(&bytes).unwrap(), r);
        assert_eq!(bytes.windows(8).any(|s| s == b"VBMAN002"), i == 1);
        assert!(matches!(
            commit(&mut pd, 220 + i as u128, bytes.clone()).outcome,
            DirectoryOutcome::ChildSlotRetired(_)
        ));
        pd = recover(&pd);
        assert!(commit(&mut pd, 220 + i as u128, bytes).duplicate);
        before = r.after().unwrap();
        assert_eq!(pd.manifest(before.input().responsibility), Some(&before));
    }
    for (group_id, child_id) in [(90, 11), (2, 90)] {
        let c = GroupCreationIntent {
            authority: group(1),
            parent: before.input().responsibility,
            expected: before.input().generation,
            responsibility: ResponsibilityIdentity {
                id: ResponsibilityId::new(child_id).unwrap(),
                incarnation: ResponsibilityIncarnation::new(2).unwrap(),
            },
            bootstrap: support::bootstrap(group_id, 3),
            application: before.input().application,
            mode: GroupCreationMode::Staging,
        };
        assert_eq!(
            commit(&mut pd, 300 + group_id, c.encode(200000).unwrap()).outcome,
            DirectoryOutcome::CreationConflict
        );
    }
    assert_eq!(pd.manifest(before.input().responsibility), Some(&before));
    // A fresh schema10 grant may also deliberately start empty; old profiles cannot.
    let mut fresh_grant = before.into_input();
    fresh_grant.generation = RouteGeneration::new(1).unwrap();
    let d = initial(vec![ResponsibilityManifest::new(fresh_grant).unwrap()], 8);
    assert!(d.is_initialized());
    assert!(d.plan().manifests().next().unwrap().has_vacancies());
}
#[cfg(feature = "native")]
#[test]
fn retirement_native_journal_cuts_recover_only_original_or_completed_routes() {
    use support::{Fault, ModelIo};
    use voteboat::{log::*, native::log_store::*};
    let (parent, child) = tree(true);
    let mut cd = initial(vec![child.clone()], 16);
    let (fact, ca) = deleted_child(&mut cd, &child);
    let r = RetireChildSlot {
        before: parent.clone(),
        child: fact,
    };
    let after = r.after().unwrap();
    let d = initial(vec![parent.clone()], 16);
    let prefix = vec![
        entry(1, 1000, d.bootstrap_command(200000).unwrap()),
        entry(
            2,
            1001,
            DirectoryCommand {
                expected: None,
                manifest: parent.clone(),
            }
            .encode(200000)
            .unwrap(),
        ),
    ];
    let bytes = r.encode(200000).unwrap();
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
                2,
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
        3,
        Some(Suffix {
            from: 3,
            entries: vec![entry(3, 220, bytes.clone())],
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
        let mut app = fresh(vec![parent.clone()], 16);
        app.apply_batch(
            &state
                .entries
                .into_iter()
                .filter(|e| e.index <= state.commit_index)
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let completed = state.commit_index == 3;
        if completed {
            complete = true;
        } else {
            old = true;
            assert_eq!(state.commit_index, 2);
        }
        assert_eq!(
            app.manifest(parent.input().responsibility),
            Some(if completed { &after } else { &parent })
        );
        assert_eq!(
            app.retired_child_slot_at(state.commit_index, op(220))
                .unwrap()
                .is_some(),
            completed
        );
        app = recover(&app);
        let receipt = commit(&mut app, 220, bytes.clone());
        assert_eq!(receipt.duplicate, completed);
        assert_eq!(
            receipt.outcome,
            DirectoryOutcome::ChildSlotRetired(RouteGeneration::new(2).unwrap())
        );
        assert_eq!(
            app.retired_child_slot_at(app.applied_index(), op(220))
                .unwrap()
                .unwrap()
                .index,
            3
        );
        assert!(ca.fence().is_some());
    }
    assert!(old && complete);
}
