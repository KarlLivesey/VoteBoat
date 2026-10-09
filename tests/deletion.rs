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
    application::*, bucket_counter::*, deletion::*, directory::*, identity::*, routed::*,
    routing::*, transfer::*,
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
    .with_namespace_deletion()
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
    next.restore_checkpoint(9, d.applied_index(), &d.checkpoint(2000000).unwrap())
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
#[test]
fn leaf_deletion_preserves_fence_tombstone_and_original_retries() {
    let m = grant();
    let mut d = initial(vec![m.clone()], 8);
    let intent = reserve(&mut d, 200, &m);
    d = recover(&d);
    let mut a = routed();
    let boot = a.bootstrap_command(200000).unwrap();
    commit(&mut a, 100, boot);
    commit(&mut a, 1, data(1, 7));
    let RoutedOutcome::Fenced(f) = commit(&mut a, 200, encode_fence(m.input().epoch)).outcome
    else {
        panic!("F")
    };
    let c = DeletionCompletion {
        intent: intent.clone(),
        fences: vec![DeletionFenceEvidence {
            configuration: ConfigurationId::new(3).unwrap(),
            fence: f,
        }],
        children: vec![],
    };
    let bytes = c.encode(200000).unwrap();
    assert_eq!(DeletionCompletion::decode(&bytes).unwrap(), c);
    let status = finish(&mut d, 201, &c);
    d = recover(&d);
    assert_eq!(
        d.deletion_status_at(d.applied_index(), op(200)).unwrap(),
        Some(status.clone())
    );
    assert_eq!(
        d.manifest(m.input().responsibility),
        Some(&intent.intent.tombstone().unwrap())
    );
    assert!(commit(&mut d, 200, intent.intent.encode(200000).unwrap()).duplicate);
    assert!(commit(&mut d, 201, bytes.clone()).duplicate);
    assert_eq!(
        commit(&mut d, 202, bytes).outcome,
        DirectoryOutcome::TransferEvidenceMismatch
    );
    assert_eq!(
        commit(
            &mut d,
            203,
            DeletionIntent { before: m.clone() }.encode(200000).unwrap()
        )
        .outcome,
        DirectoryOutcome::LifecycleBusy
    );
    let r = commit(&mut a, 2, data(1, 99));
    assert_eq!(r.outcome, RoutedOutcome::Rejected(RoutingError::Fenced));
    let mut next = routed();
    next.restore_checkpoint(
        a.schema_version(),
        a.applied_index(),
        &a.checkpoint(200000).unwrap(),
    )
    .unwrap();
    assert_eq!(next.fence(), Some(f));
    assert_eq!(next.application().outbox().count(), 1);
    assert_eq!(
        next.check_context(&hint(1), &[1]),
        Err(RoutingError::Fenced)
    );
    assert_eq!(
        commit(&mut next, 200, encode_fence(m.input().epoch)).outcome,
        RoutedOutcome::Fenced(f)
    );
    assert_eq!(status.intent, intent);
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
fn recursive(foreign: bool) {
    let (parent, child) = tree(foreign);
    let mut pd = initial(
        if foreign {
            vec![parent.clone()]
        } else {
            vec![parent.clone(), child.clone()]
        },
        16,
    );
    let pi = reserve(&mut pd, 200, &parent);
    let mut cd = if foreign {
        initial(vec![child.clone()], 16)
    } else {
        pd.clone()
    };
    let ci = reserve(&mut cd, 210, &child);
    let mut ca = owner(child.clone(), 21, child.input().scope);
    let cf = fence(&mut ca, 210);
    let cs = finish(
        &mut cd,
        211,
        &DeletionCompletion {
            intent: ci,
            fences: vec![cf],
            children: vec![],
        },
    );
    cd = recover(&cd);
    assert_eq!(
        cd.deletion_status_at(cd.applied_index(), op(210)).unwrap(),
        Some(cs.clone())
    );
    let child_fact =
        ChildDeletionEvidence::from_status(ConfigurationId::new(3).unwrap(), &cs).unwrap();
    let mut pa = owner(parent.clone(), 20, range(128, 256));
    let pf = fence(&mut pa, 200);
    let complete = DeletionCompletion {
        intent: pi,
        fences: vec![pf],
        children: vec![child_fact],
    };
    let mut missing = complete.clone();
    missing.children.clear();
    assert!(missing.encode(200000).is_err());
    if !foreign {
        assert_eq!(
            commit(&mut pd, 250, complete.encode(200000).unwrap()).outcome,
            DirectoryOutcome::TransferEvidenceMismatch
        );
        pd = cd;
    }
    let ps = finish(&mut pd, 201, &complete);
    pd = recover(&pd);
    assert_eq!(
        pd.deletion_status_at(pd.applied_index(), op(200)).unwrap(),
        Some(ps)
    );
    assert_eq!(
        pd.manifest(parent.input().responsibility)
            .unwrap()
            .input()
            .state,
        ResponsibilityState::Fenced
    );
    assert_eq!(
        ca.check_context(
            &{
                let mut h = hint(1);
                h.group = group(21);
                h.responsibility = child.input().responsibility;
                h.scope = child.input().scope;
                h
            },
            &[1]
        ),
        Err(RoutingError::Fenced)
    );
    assert!(commit(&mut pd, 201, complete.encode(200000).unwrap()).duplicate);
    let mut wrong = complete.clone();
    wrong.children[0].index += 1;
    if !foreign {
        let mut unfinished = initial(vec![parent.clone(), child.clone()], 16);
        reserve(&mut unfinished, 200, &parent);
        assert_eq!(
            commit(&mut unfinished, 202, wrong.encode(200000).unwrap()).outcome,
            DirectoryOutcome::TransferEvidenceMismatch
        );
    }
    wrong = complete.clone();
    wrong.children[0].epoch = OwnershipEpoch::new(2).unwrap();
    assert!(wrong.encode(200000).is_err());
    wrong = complete.clone();
    wrong.children.push(child_fact);
    assert!(wrong.encode(200000).is_err());
}
#[test]
fn recursive_same_authority_requires_actual_local_child_tombstone() {
    recursive(false)
}
#[test]
fn recursive_foreign_authority_binds_original_child_observation() {
    recursive(true)
}
#[test]
fn deletion_codecs_checkpoints_and_profiles_refuse_partial_or_changed_history() {
    let m = grant();
    let mut d = initial(vec![m.clone()], 8);
    let intent = reserve(&mut d, 200, &m);
    let mut a = routed();
    let f = fence(&mut a, 200);
    let c = DeletionCompletion {
        intent: intent.clone(),
        fences: vec![f],
        children: vec![],
    };
    finish(&mut d, 201, &c);
    let ib = intent.intent.encode(200000).unwrap();
    for n in 0..ib.len() {
        assert!(DeletionIntent::decode(&ib[..n]).is_err());
    }
    let cb = c.encode(200000).unwrap();
    for n in 0..cb.len() {
        assert!(DeletionCompletion::decode(&cb[..n]).is_err());
    }
    let mut extra = cb.clone();
    extra.push(0);
    assert!(DeletionCompletion::decode(&extra).is_err());
    assert!(c.encode(cb.len() - 1).is_err());
    let mut bad = c.clone();
    bad.fences[0].fence.operation = op(99);
    assert!(bad.encode(200000).is_err());
    let snap = d.checkpoint(2000000).unwrap();
    for n in 0..snap.len() {
        let mut next = fresh(vec![m.clone()], 8);
        assert!(next
            .restore_checkpoint(9, d.applied_index(), &snap[..n])
            .is_err());
        assert_eq!(next.applied_index(), 0);
    }
    let mut corrupt = snap.clone();
    let n = corrupt.len() - 1;
    corrupt[n] ^= 1;
    assert!(fresh(vec![m.clone()], 8)
        .restore_checkpoint(9, d.applied_index(), &corrupt)
        .is_err());
    for profile in 1..=8 {
        let mut old = Directory::new(
            DirectoryPlan::new(group(1), vec![m.clone()]).unwrap(),
            d.limits(),
        )
        .unwrap();
        old = match profile {
            1 => old,
            2 => old.with_group_creation().unwrap_or_else(|_| panic!()),
            3 => old.with_namespace_creation().unwrap_or_else(|_| panic!()),
            4 => old.with_namespace_transfers().unwrap_or_else(|_| panic!()),
            5 => old
                .with_responsibility_insertion()
                .unwrap_or_else(|_| panic!()),
            6 => old.with_recursive_insertion().unwrap_or_else(|_| panic!()),
            7 => old
                .with_cross_authority_insertion()
                .unwrap_or_else(|_| panic!()),
            _ => old.with_retained_insertion().unwrap_or_else(|_| panic!()),
        };
        let boot = old.bootstrap_command(200000).unwrap();
        commit(&mut old, 1000, boot);
        assert!(old
            .validate_proposal(op(200), &ib, std::iter::empty())
            .is_err());
        assert!(old
            .restore_checkpoint(profile, d.applied_index(), &snap)
            .is_err());
    }
    assert!(d.clone().with_namespace_deletion().is_err());
    let view = LifecycleDirectory::new(d.clone());
    let q = DirectoryQuery::Deletion(op(200));
    let bound = view.read_result_bound(&q).unwrap();
    let result = view.read_at(d.applied_index(), q).unwrap();
    assert!(view.read_result_bytes(&result, bound).is_ok());
    assert!(view
        .read_at(d.applied_index() + 1, DirectoryQuery::Deletion(op(200)))
        .is_err());
}
#[test]
fn reserved_completion_survives_exhausted_ordinary_capacity_and_pending_duplicates() {
    let m = grant();
    let mut d = initial(vec![m.clone()], 3);
    let intent = reserve(&mut d, 200, &m);
    assert_eq!(d.remaining_operations(), 0);
    let mut a = routed();
    let f = fence(&mut a, 200);
    let c = DeletionCompletion {
        intent,
        fences: vec![f],
        children: vec![],
    };
    let bytes = c.encode(200000).unwrap();
    assert!(d
        .validate_proposal(op(201), &bytes, [(op(201), bytes.as_slice())].into_iter())
        .is_ok());
    assert!(d
        .validate_proposal(op(202), &bytes, [(op(201), bytes.as_slice())].into_iter())
        .is_err());
    finish(&mut d, 201, &c);
    assert_eq!(d.remaining_operations(), 0);
    assert_eq!(d.reserved_publication_bytes(), 0);
    assert_eq!(recover(&d).remaining_operations(), 0);
}
#[cfg(feature = "native")]
#[test]
fn deletion_native_intent_and_tombstone_frame_cuts_recover_exact_original_state() {
    use support::{Fault, ModelIo};
    use voteboat::{log::*, native::log_store::*};
    let m = grant();
    let d = initial(vec![m.clone()], 8);
    let mut ready = d.clone();
    let intent = reserve(&mut ready, 200, &m);
    let mut a = routed();
    let f = fence(&mut a, 200);
    let c = DeletionCompletion {
        intent,
        fences: vec![f],
        children: vec![],
    };
    let prefix_all = [
        entry(1, 1000, d.bootstrap_command(200000).unwrap()),
        entry(
            2,
            1001,
            DirectoryCommand {
                expected: None,
                manifest: m.clone(),
            }
            .encode(200000)
            .unwrap(),
        ),
        entry(3, 200, c.intent.intent.encode(200000).unwrap()),
        entry(4, 201, c.encode(200000).unwrap()),
    ];
    for boundary in [3usize, 4] {
        let prefix = prefix_all[..boundary - 1].to_vec();
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
                    prefix.len() as u64,
                    Some(Suffix {
                        from: 1,
                        entries: prefix.clone(),
                    }),
                )],
            );
            (io, log)
        };
        let (_, log) = seed();
        let state = log.state(group(1)).unwrap();
        let mutation = support::update(
            &state,
            1,
            boundary as u64,
            Some(Suffix {
                from: boundary as u64,
                entries: vec![prefix_all[boundary - 1].clone()],
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
            let mut app = fresh(vec![m.clone()], 8);
            app.apply_batch(
                &state
                    .entries
                    .into_iter()
                    .filter(|e| e.index <= state.commit_index)
                    .collect::<Vec<_>>(),
            )
            .unwrap();
            if state.commit_index == boundary as u64 - 1 {
                old = true;
            } else {
                complete = true;
                assert_eq!(state.commit_index, boundary as u64);
            }
            let active = state.commit_index < 4;
            assert_eq!(
                app.manifest(m.input().responsibility)
                    .unwrap()
                    .input()
                    .state,
                if active {
                    ResponsibilityState::Active
                } else {
                    ResponsibilityState::Fenced
                }
            );
            assert_eq!(
                app.deletion_status_at(state.commit_index, op(200))
                    .unwrap()
                    .is_some(),
                !active
            );
            app = recover(&app);
            let id = if boundary == 3 { 200 } else { 201 };
            let b = if boundary == 3 {
                c.intent.intent.encode(200000).unwrap()
            } else {
                c.encode(200000).unwrap()
            };
            let retry = commit(&mut app, id, b);
            assert_eq!(retry.duplicate, state.commit_index == boundary as u64);
            assert_eq!(
                retry.outcome,
                if boundary == 3 {
                    DirectoryOutcome::DeletionIntentRecorded
                } else {
                    DirectoryOutcome::Deleted(RouteGeneration::new(2).unwrap())
                }
            );
        }
        assert!(old && complete);
    }
}
#[test]
fn deletion_locks_original_manifest_and_rejects_unresolved_creation() {
    let m = grant();
    let mut d = initial(vec![m.clone()], 16);
    let intent = reserve(&mut d, 200, &m);
    assert_eq!(
        commit(&mut d, 300, fixture::intent().encode(200000).unwrap()).outcome,
        DirectoryOutcome::LifecycleBusy
    );
    let mut edit = m.clone().into_input();
    edit.generation = RouteGeneration::new(2).unwrap();
    assert_eq!(
        commit(
            &mut d,
            301,
            DirectoryCommand {
                expected: Some(m.input().generation),
                manifest: ResponsibilityManifest::new(edit).unwrap()
            }
            .encode(200000)
            .unwrap()
        )
        .outcome,
        DirectoryOutcome::LifecycleBusy
    );
    let create = GroupCreationIntent {
        authority: group(1),
        parent: m.input().responsibility,
        expected: m.input().generation,
        responsibility: ResponsibilityIdentity {
            id: ResponsibilityId::new(90).unwrap(),
            incarnation: ResponsibilityIncarnation::new(1).unwrap(),
        },
        bootstrap: support::bootstrap(90, 3),
        application: m.input().application,
        mode: GroupCreationMode::Staging,
    };
    assert_eq!(
        commit(&mut d, 302, create.encode(200000).unwrap()).outcome,
        DirectoryOutcome::LifecycleBusy
    );
    assert_eq!(
        recover(&d)
            .deletion_intent_at(d.applied_index(), op(200))
            .unwrap(),
        Some(intent)
    );
    let mut d = initial(vec![m.clone()], 16);
    assert_eq!(
        commit(&mut d, 90, create.encode(200000).unwrap()).outcome,
        DirectoryOutcome::CreationReserved
    );
    assert_eq!(
        commit(
            &mut d,
            200,
            DeletionIntent { before: m }.encode(200000).unwrap()
        )
        .outcome,
        DirectoryOutcome::LifecycleBusy
    );
}
#[test]
fn three_authority_deletion_propagates_only_original_published_tombstones() {
    let (parent, child) = tree(true);
    let mut g = child.clone().into_input();
    g.responsibility.id = ResponsibilityId::new(12).unwrap();
    g.authority = group(3);
    g.parent = Some(ParentAuthority {
        responsibility: child.input().responsibility,
        group: group(2),
    });
    g.scope = range(0, 64);
    g.execution = ExecutionMode::Single(group(22));
    let grand = ResponsibilityManifest::new(g).unwrap();
    let mut middle = child.into_input();
    middle.execution = ExecutionMode::Delegated(vec![
        RouteEntry {
            scope: range(0, 64),
            target: RouteTarget::Child(ChildAuthority {
                responsibility: grand.input().responsibility,
                group: group(3),
                epoch: grand.input().epoch,
            }),
        },
        RouteEntry {
            scope: range(64, 128),
            target: RouteTarget::Group(group(21)),
        },
    ]);
    let middle = ResponsibilityManifest::new(middle).unwrap();
    let mut pd = initial(vec![parent.clone()], 16);
    let pi = reserve(&mut pd, 200, &parent);
    let mut md = initial(vec![middle.clone()], 16);
    let mi = reserve(&mut md, 210, &middle);
    let mut gd = initial(vec![grand.clone()], 16);
    let gi = reserve(&mut gd, 220, &grand);
    let mut ga = owner(grand.clone(), 22, grand.input().scope);
    let gf = fence(&mut ga, 220);
    let gs = finish(
        &mut gd,
        221,
        &DeletionCompletion {
            intent: gi,
            fences: vec![gf],
            children: vec![],
        },
    );
    gd = recover(&gd);
    assert_eq!(
        gd.deletion_status_at(gd.applied_index(), op(220)).unwrap(),
        Some(gs.clone())
    );
    let mut ma = owner(middle.clone(), 21, range(64, 128));
    let mf = fence(&mut ma, 210);
    let mc = DeletionCompletion {
        intent: mi,
        fences: vec![mf],
        children: vec![
            ChildDeletionEvidence::from_status(ConfigurationId::new(3).unwrap(), &gs).unwrap(),
        ],
    };
    let ms = finish(&mut md, 211, &mc);
    md = recover(&md);
    assert_eq!(
        md.deletion_status_at(md.applied_index(), op(210)).unwrap(),
        Some(ms.clone())
    );
    let mut pa = owner(parent, 20, range(128, 256));
    let pf = fence(&mut pa, 200);
    let pc = DeletionCompletion {
        intent: pi,
        fences: vec![pf],
        children: vec![
            ChildDeletionEvidence::from_status(ConfigurationId::new(3).unwrap(), &ms).unwrap(),
        ],
    };
    let ps = finish(&mut pd, 201, &pc);
    assert_eq!(
        recover(&pd)
            .deletion_status_at(pd.applied_index(), op(200))
            .unwrap(),
        Some(ps)
    );
    assert_eq!(pc.children.len(), 1);
    assert_eq!(mc.children.len(), 1);
    assert!(ma.fence().is_some() && ga.fence().is_some() && pa.fence().is_some());
}
#[test]
fn partitioned_deletion_fences_every_distinct_owner_and_refuses_wrong_coverage() {
    let mut m = grant().into_input();
    m.execution = ExecutionMode::Partitioned(vec![
        RouteEntry {
            scope: range(0, 64),
            target: RouteTarget::Group(group(20)),
        },
        RouteEntry {
            scope: range(64, 128),
            target: RouteTarget::Group(group(21)),
        },
        RouteEntry {
            scope: range(128, 256),
            target: RouteTarget::Group(group(20)),
        },
    ]);
    let m = ResponsibilityManifest::new(m).unwrap();
    let mut d = initial(vec![m.clone()], 8);
    let intent = reserve(&mut d, 200, &m);
    let mut a = owner(m.clone(), 20, m.input().scope);
    let mut b = owner(m.clone(), 21, range(64, 128));
    let c = DeletionCompletion {
        intent,
        fences: vec![fence(&mut a, 200), fence(&mut b, 200)],
        children: vec![],
    };
    let mut wrong = c.clone();
    wrong.fences.pop();
    assert!(wrong.encode(200000).is_err());
    wrong = c.clone();
    wrong.fences.reverse();
    assert!(wrong.encode(200000).is_err());
    wrong = c.clone();
    wrong.fences[0].fence.index = 0;
    assert!(wrong.encode(200000).is_err());
    let status = finish(&mut d, 201, &c);
    assert_eq!(
        recover(&d)
            .deletion_status_at(d.applied_index(), op(200))
            .unwrap(),
        Some(status)
    );
    assert!(a.fence().is_some() && b.fence().is_some());
}
#[test]
fn deletion_and_parent_delegation_serialize_and_failed_batches_keep_original_state() {
    use voteboat::delegation::DelegationPlan;
    let (parent, child) = tree(true);
    let mut after = child.clone().into_input();
    after.epoch = OwnershipEpoch::new(2).unwrap();
    after.generation = RouteGeneration::new(2).unwrap();
    after.execution = ExecutionMode::Partitioned(vec![
        RouteEntry {
            scope: range(0, 64),
            target: RouteTarget::Group(group(30)),
        },
        RouteEntry {
            scope: range(64, 128),
            target: RouteTarget::Group(group(31)),
        },
    ]);
    let plan = DelegationPlan::new(
        parent.clone(),
        child,
        ResponsibilityManifest::new(after).unwrap(),
        op(500),
    )
    .unwrap();
    let mut d = initial(vec![parent.clone()], 16);
    let original = d.applied_index();
    let intent = DeletionIntent {
        before: parent.clone(),
    };
    let b = intent.encode(200000).unwrap();
    assert!(d
        .apply_batch(&[
            entry(original + 1, 200, b.clone()),
            entry(original + 2, 999, vec![0])
        ])
        .is_err());
    assert_eq!(d.applied_index(), original);
    assert!(d.deletion_intent_at(original, op(200)).unwrap().is_none());
    assert_eq!(d.reserved_publication_bytes(), 0);
    reserve(&mut d, 200, &parent);
    assert_eq!(
        commit(&mut d, 400, plan.encode(200000).unwrap()).outcome,
        DirectoryOutcome::LifecycleBusy
    );
    let mut d = initial(vec![parent], 16);
    assert_eq!(
        commit(&mut d, 400, plan.encode(200000).unwrap()).outcome,
        DirectoryOutcome::DelegationReserved
    );
    assert_eq!(
        commit(&mut d, 200, b).outcome,
        DirectoryOutcome::LifecycleBusy
    );
    assert!(recover(&d)
        .deletion_intent_at(d.applied_index(), op(200))
        .unwrap()
        .is_none());
}
#[test]
fn fixed_owner_fence_read_preserves_original_schema_and_data_guards() {
    use std::mem::size_of;
    let mut view = RoutedControlReads::new(routed());
    assert!(view.validate_group(group(21)).is_err());
    assert_eq!(
        view.deployment_requirements(),
        routed().deployment_requirements()
    );
    let q = RoutedControlQuery::<Vec<u8>>::Fence;
    assert_eq!(view.query_bytes(&q, 0).unwrap(), 0);
    assert_eq!(
        view.read_result_bound(&q).unwrap(),
        size_of::<RoutedControlRead<i64>>()
    );
    assert_eq!(
        view.read_at(0, q.clone()).unwrap(),
        RoutedControlRead::Fence(None)
    );
    assert!(view.read_at(1, q.clone()).is_err());
    let boot = view.routed().bootstrap_command(200000).unwrap();
    commit(&mut view, 100, boot);
    commit(&mut view, 1, data(1, 7));
    let dq = RoutedControlQuery::Data(RoutedQuery {
        hint: hint(1),
        key: vec![1],
        query: vec![1],
    });
    assert_eq!(
        view.read_at(view.applied_index(), dq.clone()).unwrap(),
        RoutedControlRead::Data(RoutedRead::Served(7))
    );
    assert!(view.query_bytes(&dq, 1).is_err());
    let RoutedOutcome::Fenced(f) =
        commit(&mut view, 200, encode_fence(grant().input().epoch)).outcome
    else {
        panic!("fence")
    };
    assert_eq!(
        view.read_at(view.applied_index(), q.clone()).unwrap(),
        RoutedControlRead::Fence(Some(f))
    );
    assert_eq!(
        view.read_result_bytes(&RoutedControlRead::Fence(Some(f)), 0)
            .unwrap(),
        0
    );
    assert_eq!(
        view.read_at(view.applied_index(), dq).unwrap(),
        RoutedControlRead::Data(RoutedRead::Rejected(RoutingError::Fenced))
    );
    assert_eq!(
        view.checkpoint(200000).unwrap(),
        view.routed().checkpoint(200000).unwrap()
    );
    let mut plain = routed();
    plain
        .restore_checkpoint(
            view.schema_version(),
            view.applied_index(),
            &view.checkpoint(200000).unwrap(),
        )
        .unwrap();
    assert_eq!(plain.fence(), Some(f));
    let mut restored = RoutedControlReads::new(routed());
    restored
        .restore_checkpoint(
            plain.schema_version(),
            plain.applied_index(),
            &plain.checkpoint(200000).unwrap(),
        )
        .unwrap();
    assert_eq!(
        restored.read_at(restored.applied_index(), q).unwrap(),
        RoutedControlRead::Fence(Some(f))
    );
    assert_eq!(restored.into_routed().fence(), Some(f));
}
#[test]
fn fence_read_view_does_not_promote_a_scoped_fence_to_whole_owner_evidence() {
    let base = routed()
        .with_scoped_fencing(2)
        .unwrap_or_else(|_| panic!("scoped"));
    let mut view = RoutedControlReads::new(base);
    let boot = view.routed().bootstrap_command(200000).unwrap();
    commit(&mut view, 100, boot);
    assert!(matches!(
        commit(
            &mut view,
            200,
            encode_scope_fence(grant().input().epoch, range(0, 128))
        )
        .outcome,
        RoutedOutcome::ScopeFenced(_)
    ));
    assert_eq!(
        view.read_at(view.applied_index(), RoutedControlQuery::Fence)
            .unwrap(),
        RoutedControlRead::Fence(None)
    );
    let mut restored = RoutedControlReads::new(
        routed()
            .with_scoped_fencing(2)
            .unwrap_or_else(|_| panic!("scoped")),
    );
    restored
        .restore_checkpoint(
            view.schema_version(),
            view.applied_index(),
            &view.checkpoint(200000).unwrap(),
        )
        .unwrap();
    assert_eq!(restored.schema_version(), SCOPED_ROUTED_APPLICATION_SCHEMA);
    assert_eq!(
        restored
            .read_at(restored.applied_index(), RoutedControlQuery::Fence)
            .unwrap(),
        RoutedControlRead::Fence(None)
    );
    assert_eq!(
        restored
            .read_at(
                restored.applied_index(),
                RoutedControlQuery::Data(RoutedQuery {
                    hint: hint(1),
                    key: vec![1],
                    query: vec![1]
                })
            )
            .unwrap(),
        RoutedControlRead::Data(RoutedRead::Rejected(RoutingError::Fenced))
    );
}
