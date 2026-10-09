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
    application::*, bucket_counter::*, deletion::*, directory::*, identity::*, reparent_guard::*,
    routed::*, routing::*, transfer::*,
};
fn id(n: u128) -> ResponsibilityIdentity {
    ResponsibilityIdentity {
        id: ResponsibilityId::new(n).unwrap(),
        incarnation: ResponsibilityIncarnation::new(1).unwrap(),
    }
}
fn fixture() -> CrossReparentPlan {
    let mut child = grant().into_input();
    child.responsibility = id(11);
    child.authority = group(3);
    child.scope = range(0, 128);
    child.parent = Some(ParentAuthority {
        responsibility: id(10),
        group: group(1),
    });
    child.execution = ExecutionMode::Single(group(21));
    let child = ResponsibilityManifest::new(child).unwrap();
    let mut old = grant().into_input();
    old.execution = ExecutionMode::Delegated(vec![
        RouteEntry {
            scope: range(0, 128),
            target: RouteTarget::Child(ChildAuthority {
                responsibility: id(11),
                group: group(3),
                epoch: child.input().epoch,
            }),
        },
        RouteEntry {
            scope: range(128, 256),
            target: RouteTarget::Group(group(20)),
        },
    ]);
    let old = ResponsibilityManifest::new(old).unwrap();
    let mut new = grant().into_input();
    new.responsibility = id(30);
    new.authority = group(2);
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
    CrossReparentPlan::new(
        id(10),
        id(30),
        id(11),
        vec![old, ResponsibilityManifest::new(new).unwrap(), child],
    )
    .unwrap()
}
fn commit<A: StateMachine>(a: &mut A, operation: u128, bytes: Vec<u8>) -> A::Receipt {
    a.apply_batch(&[entry(a.applied_index() + 1, operation, bytes)])
        .unwrap()
        .remove(0)
}
fn fresh(g: u128, p: &CrossReparentPlan, operations: usize) -> Directory {
    let mut ms = p
        .manifests()
        .iter()
        .filter(|m| m.input().authority == group(g))
        .cloned()
        .collect::<Vec<_>>();
    if g == 1 {
        let mut m = grant().into_input();
        m.responsibility = id(50);
        m.execution = ExecutionMode::Single(group(51));
        ms.push(ResponsibilityManifest::new(m).unwrap());
    }
    Directory::new(
        DirectoryPlan::new(group(g), ms).unwrap(),
        DirectoryLimits {
            operations,
            history_bytes: 200000,
        },
    )
    .unwrap()
    .with_reparent_guards()
    .unwrap_or_else(|_| panic!("schema12"))
}
fn initial(g: u128, p: &CrossReparentPlan, operations: usize) -> Directory {
    let mut d = fresh(g, p, operations);
    let boot = d.bootstrap_command(200000).unwrap();
    commit(&mut d, 1000, boot);
    let ms = d.plan().manifests().cloned().collect::<Vec<_>>();
    for (i, m) in ms.into_iter().enumerate() {
        assert!(matches!(
            commit(
                &mut d,
                1001 + i as u128,
                DirectoryCommand {
                    expected: None,
                    manifest: m
                }
                .encode(200000)
                .unwrap()
            )
            .outcome,
            DirectoryOutcome::Published(_)
        ));
    }
    d
}
fn reopen(d: &Directory, p: &CrossReparentPlan) -> Directory {
    let mut r = fresh(d.plan().authority().id.get(), p, d.limits().operations);
    r.restore_checkpoint(12, d.applied_index(), &d.checkpoint(2000000).unwrap())
        .unwrap();
    assert_eq!(
        r.checkpoint(2000000).unwrap(),
        d.checkpoint(2000000).unwrap()
    );
    r
}
fn prepare(plan: &CrossReparentPlan, coordinator: Option<ReparentGuardEvidence>) -> Vec<u8> {
    PrepareReparent {
        plan: plan.clone(),
        coordinator,
    }
    .encode(MAX_REPARENT_PREPARE_BYTES)
    .unwrap()
}
fn start(coordinator: &mut Directory, p: &CrossReparentPlan) -> ReparentGuardEvidence {
    assert_eq!(
        commit(coordinator, 200, prepare(p, None)).outcome,
        DirectoryOutcome::ReparentGuarded
    );
    ReparentGuardEvidence::from_status(
        ConfigurationId::new(3).unwrap(),
        &coordinator
            .reparent_guard_at(coordinator.applied_index(), op(200))
            .unwrap()
            .unwrap(),
    )
    .unwrap()
}
fn cancel(d: &mut Directory) -> ReparentCancellationStatus {
    assert_eq!(
        commit(d, 300, CancelReparent { guard: op(200) }.encode()).outcome,
        DirectoryOutcome::ReparentCancelled
    );
    d.reparent_cancellation_at(d.applied_index(), op(200))
        .unwrap()
        .unwrap()
}
#[test]
fn three_authority_preparation_cancellation_and_original_results_survive_restart() {
    let p = fixture();
    let mut a = initial(1, &p, 16);
    let mut b = initial(2, &p, 16);
    let mut c = initial(3, &p, 16);
    assert_eq!(p.authorities(), vec![group(1), group(2), group(3)]);
    assert_eq!(
        commit(&mut b, 199, prepare(&p, None)).outcome,
        DirectoryOutcome::TransferEvidenceMismatch
    );
    let proof = start(&mut a, &p);
    for d in [&mut b, &mut c] {
        assert_eq!(
            commit(d, 200, prepare(&p, Some(proof))).outcome,
            DirectoryOutcome::ReparentGuarded
        );
        assert!(d.reparent_guard_active(op(200)));
    }
    a = reopen(&a, &p);
    b = reopen(&b, &p);
    c = reopen(&c, &p);
    for d in [&a, &b, &c] {
        assert!(d.reparent_guard_active(op(200)));
        for m in p
            .manifests()
            .iter()
            .filter(|m| m.input().authority == d.plan().authority())
        {
            assert_eq!(d.manifest(m.input().responsibility), Some(m));
        }
    }
    assert_eq!(
        commit(&mut b, 299, CancelReparent { guard: op(200) }.encode()).outcome,
        DirectoryOutcome::TransferEvidenceMismatch
    );
    let decision = cancel(&mut a);
    a = reopen(&a, &p);
    assert!(!a.reparent_guard_active(op(200)));
    assert!(b.reparent_guard_active(op(200)));
    let release = ReleaseReparentGuard {
        configuration: ConfigurationId::new(3).unwrap(),
        decision,
    };
    for d in [&mut b, &mut c] {
        assert_eq!(
            commit(d, 301, release.encode().unwrap()).outcome,
            DirectoryOutcome::ReparentCancelled
        );
        assert!(!d.reparent_guard_active(op(200)));
        assert_eq!(
            d.reparent_cancellation_at(d.applied_index(), op(200))
                .unwrap(),
            Some(decision)
        );
    }
    b = reopen(&b, &p);
    c = reopen(&c, &p);
    assert_eq!(
        commit(&mut a, 200, prepare(&p, None)).outcome,
        DirectoryOutcome::ReparentGuarded
    );
    assert!(!a.reparent_guard_active(op(200)));
    for d in [&mut b, &mut c] {
        let retry = commit(d, 200, prepare(&p, Some(proof)));
        assert!(retry.duplicate);
        assert_eq!(retry.outcome, DirectoryOutcome::ReparentGuarded);
        assert!(!d.reparent_guard_active(op(200)));
    }
    let mut fake = release;
    fake.decision.guard_index += 1;
    fake.decision.index += 1;
    assert_eq!(
        commit(&mut a, 302, fake.encode().unwrap()).outcome,
        DirectoryOutcome::TransferEvidenceMismatch
    );
    let view = LifecycleDirectory::new(a);
    let q = DirectoryQuery::ReparentGuard(op(200));
    let result = view.read_at(view.applied_index(), q).unwrap();
    assert!(view.read_result_bound(&q).unwrap() > std::mem::size_of::<DirectoryRead>());
    assert!(view.read_result_bytes(&result, 0).is_err());
    assert_eq!(
        view.read_at(view.applied_index() + 1, q),
        Err(ApplicationError::NotApplied)
    );
    let q = DirectoryQuery::ReparentCancellation(op(200));
    assert_eq!(
        view.read_result_bound(&q).unwrap(),
        std::mem::size_of::<DirectoryRead>()
    );
    assert_eq!(
        view.read_result_bytes(&view.read_at(view.applied_index(), q).unwrap(), 0)
            .unwrap(),
        0
    );
}
#[test]
fn cancelled_before_prepare_blocks_delayed_acquisition_and_reserves_completion() {
    let p = fixture();
    let mut a = initial(1, &p, 4);
    let proof = start(&mut a, &p);
    assert_eq!(a.remaining_operations(), 0);
    let decision = cancel(&mut a);
    assert_eq!(a.remaining_operations(), 0);
    let mut b = initial(2, &p, 16);
    let release = ReleaseReparentGuard {
        configuration: ConfigurationId::new(3).unwrap(),
        decision,
    };
    assert_eq!(
        commit(&mut b, 301, release.encode().unwrap()).outcome,
        DirectoryOutcome::ReparentCancelled
    );
    b = reopen(&b, &p);
    assert_eq!(
        commit(&mut b, 200, prepare(&p, Some(proof))).outcome,
        DirectoryOutcome::ReparentCancelled
    );
    assert!(!b.reparent_guard_active(op(200)));
    assert_eq!(
        b.reparent_guard_at(b.applied_index(), op(200)).unwrap(),
        None
    );
    assert_eq!(
        b.reparent_cancellation_at(b.applied_index(), op(200))
            .unwrap(),
        Some(decision)
    );
    let mut c = initial(3, &p, 3);
    assert_eq!(
        commit(&mut c, 200, prepare(&p, Some(proof))).outcome,
        DirectoryOutcome::ReparentGuarded
    );
    assert_eq!(c.remaining_operations(), 0);
    c.validate_proposal(op(301), &release.encode().unwrap(), std::iter::empty())
        .unwrap();
    assert_eq!(
        commit(&mut c, 301, release.encode().unwrap()).outcome,
        DirectoryOutcome::ReparentCancelled
    );
    assert_eq!(c.remaining_operations(), 0);
    assert_eq!(c.reserved_publication_bytes(), 0);
    reopen(&c, &p);
}
#[test]
fn guarded_manifests_block_metadata_and_lifecycle_but_disjoint_metadata_and_data_continue() {
    let p = fixture();
    let mut a = initial(1, &p, 32);
    let proof = start(&mut a, &p);
    let mut c = initial(3, &p, 16);
    commit(&mut c, 200, prepare(&p, Some(proof)));
    let mut changed = p.old_parent().clone().into_input();
    changed.generation = RouteGeneration::new(2).unwrap();
    changed.placement.minimum_voting_domains = 2;
    let command = DirectoryCommand {
        expected: Some(p.old_parent().input().generation),
        manifest: ResponsibilityManifest::new(changed).unwrap(),
    }
    .encode(200000)
    .unwrap();
    assert!(a
        .validate_proposal(op(201), &command, std::iter::empty())
        .is_err());
    assert_eq!(
        commit(&mut a, 201, command).outcome,
        DirectoryOutcome::LifecycleBusy
    );
    assert_eq!(a.manifest(id(10)), Some(p.old_parent()));
    assert_eq!(
        commit(
            &mut c,
            202,
            DeletionIntent {
                before: p.child().clone()
            }
            .encode(200000)
            .unwrap()
        )
        .outcome,
        DirectoryOutcome::LifecycleBusy
    );
    assert_eq!(
        c.deletion_intent_at(c.applied_index(), op(202)).unwrap(),
        None
    );
    let mut unrelated = a.manifest(id(50)).unwrap().clone().into_input();
    unrelated.generation = RouteGeneration::new(2).unwrap();
    unrelated.placement.minimum_voting_domains = 2;
    assert!(matches!(
        commit(
            &mut a,
            203,
            DirectoryCommand {
                expected: Some(RouteGeneration::new(1).unwrap()),
                manifest: ResponsibilityManifest::new(unrelated).unwrap()
            }
            .encode(200000)
            .unwrap()
        )
        .outcome,
        DirectoryOutcome::Published(_)
    ));
    let mut owner = RoutedApplication::new(
        group(21),
        p.child().clone(),
        BucketCounter::new(range(0, 128), Policy, bucket_limits()).unwrap(),
        Policy,
        RoutedLimits {
            operations: 32,
            semantic_bytes: 8192,
            payload_bytes: 1024,
            inner_checkpoint_bytes: bucket_limits().checkpoint_bound().unwrap(),
        },
    )
    .unwrap_or_else(|_| panic!("owner"));
    let boot = owner.bootstrap_command(200000).unwrap();
    commit(&mut owner, 100, boot);
    let before = [a.applied_index(), c.applied_index()];
    let hint = RouteHint {
        responsibility: id(11),
        group: group(21),
        application: p.child().input().application,
        scheme: p.child().input().scheme,
        scope: p.child().input().scope,
        bucket: 1,
        epoch: p.child().input().epoch,
        generation: p.child().input().generation,
    };
    let data = encode_routed(
        hint,
        &[1],
        &encode_add(&[1], 7, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap();
    assert!(matches!(
        commit(&mut owner, 1, data.clone()).outcome,
        RoutedOutcome::Applied(_)
    ));
    assert!(matches!(
        commit(&mut owner, 1, data).outcome,
        RoutedOutcome::Applied(_)
    ));
    assert_eq!(owner.application().outbox().count(), 1);
    assert_eq!(before, [a.applied_index(), c.applied_index()]);
    a = reopen(&a, &p);
    assert_eq!(a.manifest(id(10)), Some(p.old_parent()));
    assert_eq!(a.manifest(id(50)).unwrap().input().generation.get(), 2);
}

fn deep_plan(ancestors: usize, subtree: usize) -> Result<CrossReparentPlan, ApplicationError> {
    let base = fixture();
    let mut ms = base.manifests().to_vec();
    for i in 1..ancestors {
        let previous_id = if i == 1 {
            id(30)
        } else {
            id(30 + i as u128 - 1)
        };
        let parent_id = id(30 + i as u128);
        let authority = group(200 + i as u128);
        let previous = ms
            .iter_mut()
            .find(|m| m.input().responsibility == previous_id)
            .unwrap();
        let previous_authority = previous.input().authority;
        let epoch = previous.input().epoch;
        let mut changed = previous.clone().into_input();
        changed.parent = Some(ParentAuthority {
            responsibility: parent_id,
            group: authority,
        });
        *previous = ResponsibilityManifest::new(changed).unwrap();
        let mut parent = grant().into_input();
        parent.responsibility = parent_id;
        parent.authority = authority;
        parent.execution = ExecutionMode::Delegated(vec![RouteEntry {
            scope: range(0, 256),
            target: RouteTarget::Child(ChildAuthority {
                responsibility: previous_id,
                group: previous_authority,
                epoch,
            }),
        }]);
        ms.push(ResponsibilityManifest::new(parent).unwrap());
    }
    for i in 1..subtree {
        let previous_id = if i == 1 {
            id(11)
        } else {
            id(100 + i as u128 - 1)
        };
        let next_id = id(100 + i as u128);
        let authority = group(300 + i as u128);
        let previous = ms
            .iter_mut()
            .find(|m| m.input().responsibility == previous_id)
            .unwrap();
        let previous_authority = previous.input().authority;
        let mut changed = previous.clone().into_input();
        changed.execution = ExecutionMode::Delegated(vec![RouteEntry {
            scope: range(0, 128),
            target: RouteTarget::Child(ChildAuthority {
                responsibility: next_id,
                group: authority,
                epoch: OwnershipEpoch::new(1).unwrap(),
            }),
        }]);
        *previous = ResponsibilityManifest::new(changed).unwrap();
        let mut next = base.child().clone().into_input();
        next.responsibility = next_id;
        next.authority = authority;
        next.parent = Some(ParentAuthority {
            responsibility: previous_id,
            group: previous_authority,
        });
        ms.push(ResponsibilityManifest::new(next).unwrap());
    }
    CrossReparentPlan::new(id(10), id(30), id(11), ms)
}
#[test]
fn complete_ancestry_subtree_and_post_move_depth_are_required_and_codecs_are_strict() {
    assert!(deep_plan(31, 1).is_ok());
    assert!(deep_plan(32, 1).is_err());
    assert!(deep_plan(30, 3).is_err());
    let p = deep_plan(3, 3).unwrap();
    let bytes = p.encode(MAX_REPARENT_GUARD_BYTES).unwrap();
    assert_eq!(CrossReparentPlan::decode(&bytes).unwrap(), p);
    assert!(p.encode(bytes.len() - 1).is_err());
    for n in 0..bytes.len() {
        assert!(CrossReparentPlan::decode(&bytes[..n]).is_err());
    }
    for missing in 0..p.manifests().len() {
        let mut ms = p.manifests().to_vec();
        ms.remove(missing);
        assert!(CrossReparentPlan::new(id(10), id(30), id(11), ms).is_err());
    }
    let mut ms = p.manifests().to_vec();
    ms.push(grant());
    assert!(CrossReparentPlan::new(id(10), id(30), id(11), ms).is_err());
    let mut ms = p.manifests().to_vec();
    ms.push(ms[0].clone());
    assert!(CrossReparentPlan::new(id(10), id(30), id(11), ms).is_err());
    let mut bad = bytes.clone();
    bad[80..82].copy_from_slice(&u16::MAX.to_le_bytes());
    assert!(CrossReparentPlan::decode(&bad).is_err());
    let p = fixture();
    let mut a = initial(1, &p, 16);
    let proof = start(&mut a, &p);
    let request = prepare(&p, Some(proof));
    for n in 0..request.len() {
        assert!(PrepareReparent::decode(&request[..n]).is_err());
    }
    let cancellation = cancel(&mut a);
    let release = ReleaseReparentGuard {
        configuration: ConfigurationId::new(3).unwrap(),
        decision: cancellation,
    }
    .encode()
    .unwrap();
    for n in 0..release.len() {
        assert!(ReleaseReparentGuard::decode(&release[..n]).is_err());
    }
    let cancel_bytes = CancelReparent { guard: op(200) }.encode();
    for n in 0..cancel_bytes.len() {
        assert!(CancelReparent::decode(&cancel_bytes[..n]).is_err());
    }
    let mut stale = proof;
    stale.operation = op(201);
    let mut b = initial(2, &p, 16);
    assert_eq!(
        commit(&mut b, 200, prepare(&p, Some(stale))).outcome,
        DirectoryOutcome::TransferEvidenceMismatch
    );
}
#[test]
fn reciprocal_moves_share_guarded_ancestry_and_pending_cancellation_releases_only_its_move() {
    let mut ms = fixture().manifests().to_vec();
    let b_root = ms
        .iter_mut()
        .find(|m| m.input().responsibility == id(30))
        .unwrap();
    let mut input = b_root.clone().into_input();
    let ExecutionMode::Delegated(routes) = &mut input.execution else {
        unreachable!()
    };
    routes[0].target = RouteTarget::Child(ChildAuthority {
        responsibility: id(31),
        group: group(4),
        epoch: OwnershipEpoch::new(1).unwrap(),
    });
    *b_root = ResponsibilityManifest::new(input).unwrap();
    let a = ms
        .iter_mut()
        .find(|m| m.input().responsibility == id(11))
        .unwrap();
    let mut input = a.clone().into_input();
    input.execution = ExecutionMode::Delegated(vec![RouteEntry {
        scope: range(0, 128),
        target: RouteTarget::Vacant,
    }]);
    *a = ResponsibilityManifest::new(input).unwrap();
    let mut b = a.clone().into_input();
    b.responsibility = id(31);
    b.authority = group(4);
    b.parent = Some(ParentAuthority {
        responsibility: id(30),
        group: group(2),
    });
    ms.push(ResponsibilityManifest::new(b).unwrap());
    let move_a = CrossReparentPlan::new(id(10), id(31), id(11), ms.clone()).unwrap();
    let move_b = CrossReparentPlan::new(id(30), id(11), id(31), ms).unwrap();
    let mut d = initial(1, &move_a, 32);
    let a_command = prepare(&move_a, None);
    let b_command = prepare(&move_b, None);
    d.validate_proposal(
        op(200),
        &a_command,
        [(op(200), a_command.as_slice())].into_iter(),
    )
    .unwrap();
    assert!(d
        .validate_proposal(
            op(201),
            &b_command,
            [(op(200), a_command.as_slice())].into_iter()
        )
        .is_err());
    start(&mut d, &move_a);
    assert_eq!(
        commit(&mut d, 201, b_command.clone()).outcome,
        DirectoryOutcome::LifecycleBusy
    );
    let cancellation = CancelReparent { guard: op(200) }.encode();
    d.validate_proposal(
        op(202),
        &b_command,
        [(op(300), cancellation.as_slice())].into_iter(),
    )
    .unwrap();
    cancel(&mut d);
    assert_eq!(
        commit(&mut d, 202, b_command).outcome,
        DirectoryOutcome::ReparentGuarded
    );
    assert!(!d.reparent_guard_active(op(200)));
    assert!(d.reparent_guard_active(op(202)));
}
#[test]
fn profile_checkpoint_and_batch_failures_leave_no_partial_guards() {
    let p = fixture();
    let mut a = initial(1, &p, 16);
    let command = prepare(&p, None);
    let before = a.checkpoint(2000000).unwrap();
    let index = a.applied_index();
    assert!(a
        .apply_batch(&[
            entry(index + 1, 200, command.clone()),
            entry(index + 2, 201, vec![0])
        ])
        .is_err());
    assert_eq!(a.checkpoint(2000000).unwrap(), before);
    start(&mut a, &p);
    let cp = a.checkpoint(2000000).unwrap();
    let mut empty = fresh(1, &p, 16);
    let original = empty.checkpoint(2000000).unwrap();
    for n in 0..cp.len() {
        assert!(empty
            .restore_checkpoint(12, a.applied_index(), &cp[..n])
            .is_err());
        assert_eq!(empty.checkpoint(2000000).unwrap(), original);
    }
    let mut wrong = cp.clone();
    wrong[..8].copy_from_slice(b"VBDIR011");
    assert!(empty
        .restore_checkpoint(12, a.applied_index(), &wrong)
        .is_err());
    let mut old = Directory::new(a.plan().clone(), a.limits())
        .unwrap()
        .with_local_reparenting()
        .unwrap_or_else(|_| panic!("old profile"));
    let boot = old.bootstrap_command(200000).unwrap();
    commit(&mut old, 1000, boot);
    assert!(old
        .validate_proposal(op(200), &command, std::iter::empty())
        .is_err());
    assert!(old.restore_checkpoint(11, a.applied_index(), &cp).is_err());
    assert!(a.with_reparent_guards().is_err());
}
#[cfg(feature = "native")]
#[test]
fn native_guard_and_cancel_frames_recover_original_locks_or_complete_cancellation() {
    use support::{Fault, ModelIo};
    use voteboat::{log::*, native::log_store::*};
    let p = fixture();
    let mut a = initial(1, &p, 16);
    let proof = start(&mut a, &p);
    let decision = cancel(&mut a);
    let limits = LogLimits::default();
    for g in [1, 3] {
        let ordinary = if g == 1 { 4 } else { 3 };
        let prototype = fresh(g, &p, ordinary);
        let mut entries = vec![entry(1, 1000, prototype.bootstrap_command(200000).unwrap())];
        for (i, m) in prototype.plan().manifests().enumerate() {
            entries.push(entry(
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
        let guard_index = entries.len() as u64 + 1;
        let guard = prepare(&p, if g == 1 { None } else { Some(proof) });
        entries.push(entry(guard_index, 200, guard.clone()));
        entries.push(entry(
            guard_index + 1,
            if g == 1 { 300 } else { 301 },
            if g == 1 {
                CancelReparent { guard: op(200) }.encode()
            } else {
                ReleaseReparentGuard {
                    configuration: ConfigurationId::new(3).unwrap(),
                    decision,
                }
                .encode()
                .unwrap()
            },
        ));
        for boundary in [guard_index, guard_index + 1] {
            let seed = || {
                let io = ModelIo::default();
                let mut log =
                    NativeLogStore::create(io.clone(), support::identity(g), limits).unwrap();
                support::append(
                    &mut log,
                    vec![LogMutation::Create(support::bootstrap(g, 3))],
                );
                let state = log.state(group(g)).unwrap();
                support::append(
                    &mut log,
                    vec![support::update(
                        &state,
                        1,
                        boundary - 1,
                        Some(Suffix {
                            from: 1,
                            entries: entries[..boundary as usize - 1].to_vec(),
                        }),
                    )],
                );
                (io, log)
            };
            let (_, log) = seed();
            let mutation = support::update(
                &log.state(group(g)).unwrap(),
                1,
                boundary,
                Some(Suffix {
                    from: boundary,
                    entries: vec![entries[boundary as usize - 1].clone()],
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
                let log = NativeLogStore::recover(io, support::identity(g), limits).unwrap();
                let state = log.state(group(g)).unwrap();
                let mut app = fresh(g, &p, ordinary);
                app.apply_batch(
                    &state
                        .entries
                        .into_iter()
                        .filter(|e| e.index <= state.commit_index)
                        .collect::<Vec<_>>(),
                )
                .unwrap();
                if state.commit_index == boundary {
                    complete = true
                } else {
                    old = true;
                    assert_eq!(state.commit_index, boundary - 1);
                }
                assert_eq!(
                    app.reparent_guard_active(op(200)),
                    state.commit_index == guard_index
                );
                for m in p
                    .manifests()
                    .iter()
                    .filter(|m| m.input().authority == group(g))
                {
                    assert_eq!(app.manifest(m.input().responsibility), Some(m));
                }
                app = reopen(&app, &p);
                let cancel_seen = state.commit_index == guard_index + 1;
                assert_eq!(
                    app.reparent_cancellation_at(app.applied_index(), op(200))
                        .unwrap()
                        .is_some(),
                    cancel_seen
                );
                let receipt = commit(&mut app, 200, guard.clone());
                assert_eq!(receipt.outcome, DirectoryOutcome::ReparentGuarded);
                assert_eq!(receipt.duplicate, state.commit_index >= guard_index);
                assert_eq!(app.reparent_guard_active(op(200)), !cancel_seen);
            }
            assert!(old && complete);
        }
    }
}

#[test]
fn creation_reservations_and_local_reparent_cannot_bypass_guards() {
    let p = fixture();
    let mut a = initial(1, &p, 32);
    let creation = GroupCreationIntent {
        authority: group(1),
        parent: id(10),
        expected: p.old_parent().input().generation,
        responsibility: id(90),
        bootstrap: support::bootstrap(90, 3),
        application: p.old_parent().input().application,
        mode: GroupCreationMode::Staging,
    };
    start(&mut a, &p);
    assert_eq!(
        commit(&mut a, 210, creation.encode(200000).unwrap()).outcome,
        DirectoryOutcome::LifecycleBusy
    );
    assert_eq!(
        a.group_creation_at(a.applied_index(), group(90)).unwrap(),
        None
    );
    let mut busy = initial(1, &p, 32);
    assert_eq!(
        commit(&mut busy, 210, creation.encode(200000).unwrap()).outcome,
        DirectoryOutcome::CreationReserved
    );
    assert_eq!(
        commit(&mut busy, 200, prepare(&p, None)).outcome,
        DirectoryOutcome::LifecycleBusy
    );
    assert!(!busy.reparent_guard_active(op(200)));
    let ms = p
        .manifests()
        .iter()
        .map(|m| {
            let mut i = m.clone().into_input();
            i.authority = group(1);
            if let Some(parent) = &mut i.parent {
                parent.group = group(1)
            }
            if let ExecutionMode::Delegated(routes) = &mut i.execution {
                for r in routes {
                    if let RouteTarget::Child(c) = &mut r.target {
                        c.group = group(1)
                    }
                }
            }
            ResponsibilityManifest::new(i).unwrap()
        })
        .collect();
    let local = CrossReparentPlan::new(id(10), id(30), id(11), ms).unwrap();
    let mut d = initial(1, &local, 32);
    start(&mut d, &local);
    let atomic = voteboat::reparenting::ReparentPlan::new(
        local.old_parent().clone(),
        local.new_parent().clone(),
        local.child().clone(),
    )
    .unwrap();
    assert_eq!(
        commit(&mut d, 250, atomic.encode(200000).unwrap()).outcome,
        DirectoryOutcome::LifecycleBusy
    );
    cancel(&mut d);
    assert_eq!(
        commit(&mut d, 251, atomic.encode(200000).unwrap()).outcome,
        DirectoryOutcome::Reparented
    );
}

#[path = "reparent_guards/commit.rs"]
mod committed;
