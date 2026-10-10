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
#[path = "delegation/cancellation.rs"]
mod cancellation;
#[path = "delegation/fixtures.rs"]
mod fixture;
#[path = "delegation/repeat.rs"]
mod repeat;
#[path = "transfer_source/fixtures.rs"]
pub mod source_fixture;
use base::{entry, group, op};
use fixture::*;
use std::collections::BTreeMap;
use voteboat::{
    application::*, bucket_counter::*, delegation::*, directory::*, identity::*, routed::*,
    routing::*, transfer::*, transfer_target::*,
};
type Target = TransferTarget<BucketCounter<base::Policy>, base::Policy>;
fn refreshed_parent(epoch: u64) -> ResponsibilityManifest {
    let mut input = parent().into_input();
    input.generation = RouteGeneration::new(2).unwrap();
    let ExecutionMode::Delegated(routes) = &mut input.execution else {
        unreachable!()
    };
    let RouteTarget::Child(child) = &mut routes[0].target else {
        unreachable!()
    };
    child.epoch = OwnershipEpoch::new(epoch).unwrap();
    ResponsibilityManifest::new(input).unwrap()
}

#[test]
fn child_locator_refresh_requires_unchanged_parent_and_child_bindings() {
    let old = parent();
    let next = refreshed_parent(3); // A cache may miss intermediate publications.
    assert!(next.refreshes_child_epochs(&old));
    assert!(!old.refreshes_child_epochs(&next));
    assert!(!old.refreshes_child_epochs(&old));
    assert!(!refreshed_parent(2).refreshes_child_epochs(&next));
    for mutation in 0..8 {
        let mut input = next.clone().into_input();
        match mutation {
            0 => input.epoch = OwnershipEpoch::new(2).unwrap(),
            1 => input.parent = None,
            2 => input.state = ResponsibilityState::Fenced,
            3 => input.authority = group(101),
            6 => {
                input.scope = BucketRange::new(input.scope.start(), input.scope.end() - 1).unwrap();
                let ExecutionMode::Delegated(routes) = &mut input.execution else {
                    unreachable!()
                };
                routes[0].scope = input.scope;
            }
            7 => input.execution = ExecutionMode::Single(group(20)),
            n => {
                let ExecutionMode::Delegated(routes) = &mut input.execution else {
                    unreachable!()
                };
                let RouteTarget::Child(child) = &mut routes[0].target else {
                    unreachable!()
                };
                if n == 4 {
                    child.group = group(2);
                } else {
                    child.responsibility = id(11);
                }
            }
        }
        assert!(!ResponsibilityManifest::new(input)
            .unwrap()
            .refreshes_child_epochs(&old));
    }
}

#[cfg(feature = "native")]
#[test]
fn native_parent_cache_accepts_child_epoch_refresh_and_preserves_rejected_view() {
    use voteboat::native::routing::NativeManifestCache;
    let mut cache = NativeManifestCache::new(ManifestCacheLimits {
        manifests: 4,
        bytes: 65536,
    })
    .unwrap();
    cache.admit(parent()).unwrap();
    let next = refreshed_parent(3);
    cache.admit(next.clone()).unwrap();
    let usage = cache.usage();
    for mutation in 0..3 {
        let mut input = next.clone().into_input();
        input.generation = RouteGeneration::new(3).unwrap();
        let ExecutionMode::Delegated(routes) = &mut input.execution else {
            unreachable!()
        };
        let RouteTarget::Child(child) = &mut routes[0].target else {
            unreachable!()
        };
        match mutation {
            0 => child.epoch = OwnershipEpoch::new(2).unwrap(),
            1 => child.group = group(2),
            _ => child.responsibility = id(11),
        }
        assert!(cache
            .admit(ResponsibilityManifest::new(input).unwrap())
            .is_err());
        assert_eq!(cache.usage(), usage);
        assert_eq!(cache.get(id(500)), Some(&next));
    }
}
fn verify_independent_targets(
    targets: &mut [Target],
    cache: &Cache,
    intent: &TransferIntent,
    decision: &voteboat::transfer_publication::TransferPublicationStatus,
) {
    for (i, target) in targets.iter_mut().enumerate() {
        let activation = target
            .activation_command(
                &TargetActivation {
                    metadata_configuration: ConfigurationId::new(1).unwrap(),
                    decision: decision.clone(),
                },
                65536,
            )
            .unwrap();
        command(target, 200, activation);
        let key = if i == 0 { 1 } else { 200 };
        let hint = resolve(cache, &base::Policy, id(500), &[key], 4).unwrap();
        assert_eq!(
            resolve(cache, &base::Policy, id(600), &[key], 4).unwrap(),
            hint
        );
        assert_eq!(hint.group, group(21 + i as u128));
        let data = encode_routed(
            hint,
            &[key],
            &encode_add(&[key], if i == 0 { 7 } else { 11 }, b"effect", 1024).unwrap(),
            4096,
        )
        .unwrap();
        let TargetOutcome::Applied(retry) = command(target, 1 + i as u128, data).outcome else {
            panic!("retry")
        };
        assert!(retry.duplicate);
        let checkpoint = target.checkpoint(100000).unwrap();
        let mut recovered = fresh_target(21 + i as u128, intent);
        recovered
            .restore_checkpoint(target.schema_version(), target.applied_index(), &checkpoint)
            .unwrap();
        assert_eq!(recovered.status(), target.status());
        // Warm path starts at the child and has no parent manifest.
        let warm = Cache(BTreeMap::from([(before().input().responsibility, after())]));
        let hint = resolve(
            &warm,
            &base::Policy,
            before().input().responsibility,
            &[key],
            4,
        )
        .unwrap();
        let data = encode_routed(
            hint,
            &[key],
            &encode_add(&[key], 1, b"", 1024).unwrap(),
            4096,
        )
        .unwrap();
        assert!(matches!(
            command(&mut recovered, 10 + i as u128, data).outcome,
            TargetOutcome::Applied(_)
        ));
    }
}
#[test]
fn delegated_split_reserves_parent_and_recovers_epochs_without_ancestor_write_dependency() {
    let mut p = ready_directory(true, 3);
    let mut child = ready_directory(false, 3);
    assert_eq!(
        command(&mut p, 400, plan().encode(65536).unwrap()).outcome,
        DirectoryOutcome::DelegationReserved
    );
    p = recover_directory(&p, true, 3);
    let s = reservation(&p);
    let intent = s.child_intent(ConfigurationId::new(1).unwrap()).unwrap();
    let (mut source, mut targets, decision) = completed_child(&intent, &mut child);
    child = recover_directory(&child, false, 3);
    assert_eq!(
        child
            .directory()
            .transfer_publication_at(child.applied_index(), op(200))
            .unwrap(),
        Some(decision.clone())
    );
    let mut cache = Cache(BTreeMap::from([
        (id(600), grandparent()),
        (id(500), parent()),
        (before().input().responsibility, after()),
    ]));
    assert_eq!(
        resolve(&cache, &base::Policy, id(500), &[1], 4),
        Err(RoutingError::WrongChild)
    );
    let completion = DelegationCompletion {
        reservation: op(400),
        reservation_index: s.index,
        parent_configuration: ConfigurationId::new(1).unwrap(),
        child_configuration: ConfigurationId::new(1).unwrap(),
        decision: decision.clone(),
    };
    let bytes = completion.encode(MAX_DELEGATION_COMPLETION_BYTES).unwrap();
    // Reservation exhausted ordinary history; the final control phase remains available.
    assert_eq!(p.directory().remaining_operations(), 0);
    p.validate_proposal(op(401), &bytes, std::iter::empty())
        .unwrap();
    assert_eq!(
        command(&mut p, 401, bytes.clone()).outcome,
        DirectoryOutcome::DelegationPublished(RouteGeneration::new(2).unwrap())
    );
    assert_eq!(
        command(&mut p, 401, bytes).outcome,
        DirectoryOutcome::DelegationPublished(RouteGeneration::new(2).unwrap())
    );
    p = recover_directory(&p, true, 3);
    let updated = p.directory().manifest(id(500)).unwrap().clone();
    assert_eq!(updated.input().epoch, parent().input().epoch);
    assert_eq!(updated.input().parent, parent().input().parent);
    cache.admit(updated).unwrap();
    let parent_cp = p.checkpoint(1000000).unwrap();
    verify_independent_targets(&mut targets, &cache, &intent, &decision);
    assert_eq!(p.checkpoint(1000000).unwrap(), parent_cp);
    let mut replay = fresh_directory(true, 3);
    let completion_bytes = completion.encode(100000).unwrap();
    let log = vec![
        entry(
            1,
            1000,
            replay.directory().bootstrap_command(65536).unwrap(),
        ),
        entry(
            2,
            1001,
            DirectoryCommand {
                expected: None,
                manifest: parent(),
            }
            .encode(65536)
            .unwrap(),
        ),
        entry(3, 400, plan().encode(65536).unwrap()),
        entry(4, 401, completion_bytes.clone()),
        entry(5, 401, completion_bytes),
    ];
    replay.apply_batch(&log).unwrap();
    assert_eq!(replay.checkpoint(1000000).unwrap(), parent_cp);
    assert!(matches!(
        command(&mut source, 3, base::data(1, 1)).outcome,
        RoutedOutcome::Rejected(RoutingError::Fenced)
    ));
}

#[test]
fn same_group_parent_and_child_require_actual_local_reservation_and_publication() {
    let mut b = before().into_input();
    b.authority = group(100);
    let b = ResponsibilityManifest::new(b).unwrap();
    let mut a = after().into_input();
    a.authority = group(100);
    let a = ResponsibilityManifest::new(a).unwrap();
    let mut p = parent().into_input();
    p.execution = ExecutionMode::Delegated(vec![RouteEntry {
        scope: b.input().scope,
        target: RouteTarget::Child(ChildAuthority {
            responsibility: b.input().responsibility,
            group: group(100),
            epoch: b.input().epoch,
        }),
    }]);
    let p = ResponsibilityManifest::new(p).unwrap();
    let plan = DelegationPlan::new(p.clone(), b.clone(), a.clone(), op(200)).unwrap();
    let directory_plan = DirectoryPlan::new(group(100), vec![p.clone(), b.clone()]).unwrap();
    let fresh = || {
        LifecycleDirectory::new(
            Directory::new(
                directory_plan.clone(),
                DirectoryLimits {
                    operations: 12,
                    history_bytes: 65536,
                },
            )
            .unwrap(),
        )
    };
    let mut d = fresh();
    let boot = d.directory().bootstrap_command(65536).unwrap();
    command(&mut d, 1000, boot);
    for (operation, manifest) in [(1001, p), (1002, b)] {
        command(
            &mut d,
            operation,
            DirectoryCommand {
                expected: None,
                manifest,
            }
            .encode(65536)
            .unwrap(),
        );
    }
    command(&mut d, 400, plan.encode(65536).unwrap());
    let intent = reservation(&d)
        .child_intent(ConfigurationId::new(1).unwrap())
        .unwrap();
    let (_, _, decision) = completed_child(&intent, &mut d);
    let completion = DelegationCompletion {
        reservation: op(400),
        reservation_index: 4,
        parent_configuration: ConfigurationId::new(1).unwrap(),
        child_configuration: ConfigurationId::new(1).unwrap(),
        decision,
    };
    let mut forged = completion.clone();
    forged.decision.index += 1;
    assert_eq!(
        command(&mut d, 402, forged.encode(100000).unwrap()).outcome,
        DirectoryOutcome::TransferEvidenceMismatch
    );
    assert_eq!(
        command(&mut d, 401, completion.encode(100000).unwrap()).outcome,
        DirectoryOutcome::DelegationPublished(RouteGeneration::new(2).unwrap())
    );
    assert_eq!(d.directory().manifest(a.input().responsibility), Some(&a));
    let cp = d.checkpoint(1000000).unwrap();
    let mut restored = fresh();
    restored
        .restore_checkpoint(d.schema_version(), d.applied_index(), &cp)
        .unwrap();
    assert_eq!(restored.checkpoint(1000000).unwrap(), cp);
}
#[test]
fn parent_reservation_and_completion_bind_exact_generation_operation_and_foreign_decision() {
    assert!(TransferIntent::new(before(), after()).is_err());
    let mut stale = ready_directory(true, 12);
    let mut newer = parent().into_input();
    newer.generation = RouteGeneration::new(2).unwrap();
    assert_eq!(
        command(
            &mut stale,
            390,
            DirectoryCommand {
                expected: Some(RouteGeneration::new(1).unwrap()),
                manifest: ResponsibilityManifest::new(newer).unwrap()
            }
            .encode(65536)
            .unwrap()
        )
        .outcome,
        DirectoryOutcome::Published(RouteGeneration::new(2).unwrap())
    );
    assert_eq!(
        command(&mut stale, 400, plan().encode(65536).unwrap()).outcome,
        DirectoryOutcome::GenerationMismatch
    );
    assert!(stale
        .directory()
        .delegation_reservation_at(stale.applied_index(), op(400))
        .unwrap()
        .is_none());
    let mut p = ready_directory(true, 12);
    let bytes = plan().encode(65536).unwrap();
    assert_eq!(
        command(&mut p, 400, bytes.clone()).outcome,
        DirectoryOutcome::DelegationReserved
    );
    assert!(command(&mut p, 400, bytes.clone()).duplicate);
    assert_eq!(
        command(&mut p, 402, bytes).outcome,
        DirectoryOutcome::LifecycleBusy
    );
    let mut metadata_edit = parent().into_input();
    metadata_edit.generation = RouteGeneration::new(2).unwrap();
    assert_eq!(
        command(
            &mut p,
            403,
            DirectoryCommand {
                expected: Some(RouteGeneration::new(1).unwrap()),
                manifest: ResponsibilityManifest::new(metadata_edit).unwrap()
            }
            .encode(65536)
            .unwrap()
        )
        .outcome,
        DirectoryOutcome::LifecycleBusy
    );
    let s = reservation(&p);
    let intent = s.child_intent(ConfigurationId::new(1).unwrap()).unwrap();
    let mut child = ready_directory(false, 12);
    assert_eq!(
        command(&mut child, 999, intent.encode(65536).unwrap()).outcome,
        DirectoryOutcome::TransferEvidenceMismatch
    );
    let (_, _, decision) = completed_child(&intent, &mut child);
    let good = DelegationCompletion {
        reservation: op(400),
        reservation_index: s.index,
        parent_configuration: ConfigurationId::new(1).unwrap(),
        child_configuration: ConfigurationId::new(1).unwrap(),
        decision,
    };
    for i in 0..3 {
        let mut bad = good.clone();
        match i {
            0 => bad.reservation_index += 1,
            1 => bad.parent_configuration = ConfigurationId::new(2).unwrap(),
            _ => bad.reservation = op(999),
        };
        assert_eq!(
            command(&mut p, 410 + i, bad.encode(100000).unwrap()).outcome,
            DirectoryOutcome::TransferEvidenceMismatch
        );
        assert_eq!(p.directory().manifest(id(500)), Some(&parent()));
    }
    let mut source = fresh_source();
    let boot = source.bootstrap_command(65536).unwrap();
    command(&mut source, 100, boot);
    let before = source.checkpoint(100000).unwrap();
    assert!(source
        .apply_batch(&[entry(
            2,
            999,
            base::Source::freeze_command(&intent, 65536).unwrap()
        )])
        .is_err());
    assert_eq!(source.checkpoint(100000).unwrap(), before);
    assert_eq!(
        command(&mut p, 401, good.encode(100000).unwrap()).outcome,
        DirectoryOutcome::DelegationPublished(RouteGeneration::new(2).unwrap())
    );
}
#[test]
fn new_codecs_queries_and_checkpoint_reject_truncations_and_account_nested_capacity() {
    let bytes = plan().encode(65536).unwrap();
    assert_eq!(DelegationPlan::decode(&bytes).unwrap(), plan());
    for end in 0..bytes.len() {
        assert!(DelegationPlan::decode(&bytes[..end]).is_err());
    }
    let mut p = ready_directory(true, 3);
    command(&mut p, 400, bytes);
    let intent = reservation(&p)
        .child_intent(ConfigurationId::new(1).unwrap())
        .unwrap();
    let bytes = intent.encode(65536).unwrap();
    assert_eq!(&bytes[..8], b"VBTINT02");
    assert_eq!(bytes.capacity(), bytes.len());
    for end in 0..bytes.len() {
        assert!(TransferIntent::decode(&bytes[..end]).is_err());
    }
    assert_eq!(TransferIntent::decode(&bytes).unwrap(), intent);
    for position in 0..bytes.len() {
        let mut changed = bytes.clone();
        changed[position] ^= 1;
        assert!(
            TransferIntent::decode(&changed).is_err(),
            "changed bound intent byte {position}"
        );
    }
    let mut child = ready_directory(false, 3);
    let (_, _, decision) = completed_child(&intent, &mut child);
    let completion = DelegationCompletion {
        reservation: op(400),
        reservation_index: 3,
        parent_configuration: ConfigurationId::new(1).unwrap(),
        child_configuration: ConfigurationId::new(1).unwrap(),
        decision,
    };
    let bytes = completion.encode(100000).unwrap();
    assert_eq!(bytes.capacity(), bytes.len());
    assert_eq!(DelegationCompletion::decode(&bytes).unwrap(), completion);
    assert!(completion.encode(bytes.len() - 1).is_err());
    for end in 0..bytes.len() {
        assert!(DelegationCompletion::decode(&bytes[..end]).is_err());
    }
    command(&mut p, 401, bytes);
    for q in [
        DirectoryQuery::DelegationReservation(op(400)),
        DirectoryQuery::DelegationPublication(op(400)),
    ] {
        let result = p.read_at(p.applied_index(), q).unwrap();
        assert!(
            p.read_result_bytes(&result, usize::MAX).unwrap() + std::mem::size_of_val(&result)
                <= p.read_result_bound(&q).unwrap()
        );
        assert!(p.read_at(p.applied_index() + 1, q).is_err());
    }
    let cp = p.checkpoint(1000000).unwrap();
    let mut pristine = fresh_directory(true, 3);
    let before = pristine.checkpoint(1000000).unwrap();
    for end in 0..cp.len() {
        assert!(pristine
            .restore_checkpoint(p.schema_version(), p.applied_index(), &cp[..end])
            .is_err());
        assert_eq!(pristine.checkpoint(1000000).unwrap(), before);
    }
    recover_directory(&p, true, 3);
}

#[test]
fn parent_final_publication_keeps_its_reserve_after_ordinary_byte_and_pending_exhaustion() {
    let manifest_command = DirectoryCommand {
        expected: None,
        manifest: parent(),
    }
    .encode(65536)
    .unwrap();
    let prepare = plan().encode(65536).unwrap();
    let bootstrap_bytes = fresh_directory(true, 3)
        .directory()
        .bootstrap_command(65536)
        .unwrap()
        .len();
    let history_bytes = bootstrap_bytes + manifest_command.len() + prepare.len();
    let mut p = LifecycleDirectory::new(
        Directory::new(
            DirectoryPlan::new(group(100), vec![parent()]).unwrap(),
            DirectoryLimits {
                operations: 3,
                history_bytes,
            },
        )
        .unwrap(),
    );
    let boot = p.directory().bootstrap_command(65536).unwrap();
    command(&mut p, 1000, boot);
    command(&mut p, 1001, manifest_command);
    command(&mut p, 400, prepare);
    assert_eq!(p.directory().remaining_operations(), 0);
    assert_eq!(
        p.directory().reserved_publication_bytes(),
        MAX_DIRECTORY_CONTROL_BYTES
    );
    let s = reservation(&p);
    let intent = s.child_intent(ConfigurationId::new(1).unwrap()).unwrap();
    let mut child = ready_directory(false, 3);
    let (_, _, decision) = completed_child(&intent, &mut child);
    let completion = DelegationCompletion {
        reservation: op(400),
        reservation_index: s.index,
        parent_configuration: ConfigurationId::new(1).unwrap(),
        child_configuration: ConfigurationId::new(1).unwrap(),
        decision,
    };
    let bytes = completion.encode(100000).unwrap();
    p.validate_proposal(op(401), &bytes, std::iter::empty())
        .unwrap();
    // Two distinct final proposals cannot both spend the one reserved phase.
    assert!(p
        .validate_proposal(op(402), &bytes, [(op(401), bytes.as_slice())].into_iter())
        .is_err());
    assert_eq!(
        command(&mut p, 401, bytes).outcome,
        DirectoryOutcome::DelegationPublished(RouteGeneration::new(2).unwrap())
    );
    assert_eq!(p.directory().reserved_publication_bytes(), 0);
    let checkpoint = p.checkpoint(1000000).unwrap();
    let mut recovered = LifecycleDirectory::new(
        Directory::new(p.directory().plan().clone(), p.directory().limits()).unwrap(),
    );
    recovered
        .restore_checkpoint(p.schema_version(), p.applied_index(), &checkpoint)
        .unwrap();
    assert_eq!(recovered.checkpoint(1000000).unwrap(), checkpoint);
}
