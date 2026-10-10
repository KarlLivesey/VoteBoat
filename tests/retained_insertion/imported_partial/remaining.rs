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

use super::*;
fn directory(m: ResponsibilityManifest, _: bool) -> Directory {
    let mut d = Directory::new(
        DirectoryPlan::new(m.input().authority, vec![m.clone()]).unwrap(),
        DirectoryLimits {
            operations: 32,
            history_bytes: 200000,
        },
    )
    .unwrap()
    .with_remaining_transfer()
    .unwrap_or_else(|_| panic!("profile14"));
    let b = d.bootstrap_command(200000).unwrap();
    commit(&mut d, 1000, b);
    commit(
        &mut d,
        1001,
        DirectoryCommand {
            expected: None,
            manifest: m,
        }
        .encode(200000)
        .unwrap(),
    );
    d
}
fn remaining_after(t: &Target, split: bool) -> ResponsibilityManifest {
    let mut a = t.grant().clone().into_input();
    a.epoch = OwnershipEpoch::new(a.epoch.get() + 1).unwrap();
    a.generation = RouteGeneration::new(a.generation.get() + 1).unwrap();
    let ExecutionMode::Delegated(old) = a.execution else {
        panic!("delegated")
    };
    a.execution = ExecutionMode::Delegated(
        old.into_iter()
            .flat_map(|r| {
                if r.target != RouteTarget::Group(group(21)) {
                    return vec![r];
                }
                if split {
                    vec![
                        RouteEntry {
                            scope: range(96, 112),
                            target: RouteTarget::Group(group(40)),
                        },
                        RouteEntry {
                            scope: range(112, 128),
                            target: RouteTarget::Group(group(41)),
                        },
                    ]
                } else {
                    vec![RouteEntry {
                        scope: r.scope,
                        target: RouteTarget::Group(group(40)),
                    }]
                }
            })
            .collect(),
    );
    ResponsibilityManifest::new(a).unwrap()
}
fn child(t: &TransferIntent, g: u128) -> Target {
    let scope = t
        .targets()
        .into_iter()
        .find(|r| r.target == RouteTarget::Group(group(g)))
        .unwrap()
        .scope;
    Target::new(
        group(g),
        op(3000),
        t.clone(),
        BucketCounter::new(scope, Policy, bucket_limits()).unwrap(),
        Policy,
        TargetLimits {
            import_bytes: 32768,
            application_checkpoint_bytes: bucket_limits().checkpoint_bound().unwrap(),
        },
    )
    .unwrap_or_else(|_| panic!("remaining target"))
}
fn prepare_remaining(d: &mut Directory, t: &Target, split: bool) -> TransferIntent {
    let after = remaining_after(t, split);
    let plan = DelegationPlan::move_remaining(
        d.manifest(rid(10)).unwrap().clone(),
        t.grant().clone(),
        after,
        op(3000),
    )
    .unwrap();
    let encoded = plan.encode(200000).unwrap();
    assert_eq!(&encoded[..8], b"VBDPLAN5");
    assert_eq!(DelegationPlan::decode(&encoded).unwrap(), plan);
    assert!(matches!(
        commit(d, 6002, encoded).outcome,
        DirectoryOutcome::DelegationReserved
    ));
    let reservation = d
        .delegation_reservation_at(d.applied_index(), op(6002))
        .unwrap()
        .unwrap();
    let cfg = ConfigurationId::new(1).unwrap();
    let i = reservation.child_intent(cfg).unwrap();
    let bytes = i.encode(200000).unwrap();
    assert_eq!(&bytes[..8], b"VBTINT07");
    assert_eq!(TransferIntent::decode(&bytes).unwrap(), i);
    assert!(matches!(
        commit(d, 3000, bytes).outcome,
        DirectoryOutcome::TransferIntentRecorded
    ));
    i
}
struct Completed {
    d: Directory,
    t: Target,
    first: TransferIntent,
    intent: TransferIntent,
    log: Vec<voteboat::log::LogEntry>,
    successors: Vec<Target>,
    decision: TransferPublicationStatus,
    children: [Target; 2],
    old_children: Vec<ResponsibilityManifest>,
    frozen: Vec<u8>,
}
fn completed(split: bool) -> Completed {
    let cfg = ConfigurationId::new(1).unwrap();
    let (mut d, mut t, first, mut log) = initial_with_directory(2, directory);
    let (child_a, _, _) = transfer(&mut d, &mut t, &mut log, 0, range(0, 64));
    let (child_b, _, _) = transfer(&mut d, &mut t, &mut log, 1, range(64, 96));
    let old_children: Vec<_> = [rid(30), rid(31)]
        .into_iter()
        .map(|id| d.manifest(id).unwrap().clone())
        .collect();
    let i = prepare_remaining(&mut d, &t, split);
    let frozen = t.freeze_command(&i, 65536, 200000).unwrap();
    assert!(matches!(
        record(&mut t, &mut log, 3000, frozen.clone()),
        TargetOutcome::Frozen(_)
    ));
    let status = t.freeze_status().unwrap().unwrap();
    assert_eq!(status.exports.len(), if split { 2 } else { 1 });
    assert!(status.exports.iter().all(|e| e.scope.start() >= 96));
    let live = t.checkpoint(400000).unwrap();
    t = reopen(&t, &first, 2);
    assert_eq!(t.checkpoint(400000).unwrap(), live);
    assert!(matches!(read(&t, 100), TargetRead::Rejected(_)));
    let mut successors = Vec::new();
    for g in if split { vec![40, 41] } else { vec![40] } {
        let mut c = child(&i, g);
        let b = c.bootstrap_command(200000).unwrap();
        commit(&mut c, 3000, b);
        let image = t.export_target(group(g), 65536).unwrap();
        assert_eq!(
            image.scope(),
            i.targets()
                .into_iter()
                .find(|r| r.target == RouteTarget::Group(group(g)))
                .unwrap()
                .scope
        );
        let digest = ContentDigest::scope_image(&image);
        let import = TargetImport::new(
            op(3000),
            i.clone(),
            group(g),
            vec![SourceImport {
                fence: status.fence,
                configuration: cfg,
                image,
                digest,
            }],
        )
        .unwrap();
        let b = c.import_command(&import, 200000).unwrap();
        commit(&mut c, 3000, b);
        successors.push(c);
    }
    let p = TransferPublication::new(
        op(3000),
        i.clone(),
        vec![SourceFenceEvidence::from_status(cfg, status).unwrap()],
        successors
            .iter()
            .map(|c| TargetReadyEvidence::from_status(cfg, c.status()).unwrap())
            .collect(),
    )
    .unwrap();
    assert!(matches!(
        commit(&mut d, 3001, p.encode(200000).unwrap()).outcome,
        DirectoryOutcome::TransferPublished(_)
    ));
    let decision = d
        .transfer_publication_at(d.applied_index(), op(3000))
        .unwrap()
        .unwrap();
    let b = DelegationCompletion {
        reservation: op(6002),
        reservation_index: d
            .delegation_reservation_at(d.applied_index(), op(6002))
            .unwrap()
            .unwrap()
            .index,
        parent_configuration: cfg,
        child_configuration: cfg,
        decision: decision.clone(),
    }
    .encode(200000)
    .unwrap();
    assert!(matches!(
        commit(&mut d, 6003, b).outcome,
        DirectoryOutcome::DelegationPublished(_)
    ));
    for (j, c) in successors.iter_mut().enumerate() {
        let b = c
            .activation_command(
                &TargetActivation {
                    metadata_configuration: cfg,
                    decision: decision.clone(),
                },
                200000,
            )
            .unwrap();
        commit(c, 3000, b);
        assert_eq!(c.schema_version(), REMAINING_TRANSFER_TARGET_SCHEMA);
        let cp = c.checkpoint(400000).unwrap();
        let mut fresh = child(&i, 40 + j as u128);
        fresh
            .restore_checkpoint(c.schema_version(), c.applied_index(), &cp)
            .unwrap();
        assert_eq!(fresh.status(), c.status());
        *c = fresh;
    }
    Completed {
        d,
        t,
        first,
        intent: i,
        log,
        successors,
        decision,
        children: [child_a, child_b],
        old_children,
        frozen,
    }
}
#[test]
fn partial_imports_move_all_remaining_data_without_changing_earlier_children() {
    for split in [false, true] {
        let Completed {
            d,
            mut t,
            first: _,
            intent: i,
            mut log,
            mut successors,
            children: [child_a, child_b],
            old_children,
            frozen,
            ..
        } = completed(split);
        let c = &mut successors[0];
        assert_eq!(c.application().value(&[100]), Ok(9));
        let m = i.after().input();
        let h = RouteHint {
            responsibility: m.responsibility,
            group: group(40),
            application: m.application,
            scheme: m.scheme,
            scope: if split {
                range(96, 112)
            } else {
                range(96, 128)
            },
            bucket: 100,
            epoch: m.epoch,
            generation: m.generation,
        };
        let b = encode_routed(
            h,
            &[100],
            &encode_add(&[100], 9, b"effect", 1024).unwrap(),
            4096,
        )
        .unwrap();
        assert!(matches!(commit(c,2,b).outcome,TargetOutcome::Applied(r) if r.duplicate));
        let b = encode_routed(
            h,
            &[100],
            &encode_add(&[100], 3, b"effect", 1024).unwrap(),
            4096,
        )
        .unwrap();
        assert!(
            matches!(commit(c,99,b).outcome,TargetOutcome::Applied(r) if !r.duplicate && r.outcome == BucketOutcome::Value(12))
        );
        assert_eq!(child_a.application().value(&[1]), Ok(7));
        assert_eq!(child_b.application().outbox().count(), 0);
        for m in old_children {
            assert_eq!(d.manifest(m.input().responsibility), Some(&m));
        }
        let mut recovered = directory(before(false), false);
        recovered
            .restore_checkpoint(14, d.applied_index(), &d.checkpoint(400000).unwrap())
            .unwrap();
        assert_eq!(recovered.manifest(rid(21)), d.manifest(rid(21)));
        assert!(matches!(
            record(&mut t, &mut log, 3000, frozen),
            TargetOutcome::Frozen(_)
        ));
    }
}

#[test]
fn remaining_moves_reject_changed_children_reused_groups_and_legacy_profiles() {
    let (mut d, mut t, first, mut log) = initial_with_directory(2, directory);
    let _ = transfer(&mut d, &mut t, &mut log, 0, range(0, 64));
    let _ = transfer(&mut d, &mut t, &mut log, 1, range(64, 96));
    let after = remaining_after(&t, true);
    let parent = d.manifest(rid(10)).unwrap().clone();
    let make =
        |after| DelegationPlan::move_remaining(parent.clone(), t.grant().clone(), after, op(3000));
    let plan = make(after.clone()).unwrap();
    let encoded = plan.encode(200000).unwrap();
    for end in 0..encoded.len() {
        assert!(DelegationPlan::decode(&encoded[..end]).is_err());
    }
    let mut old = encoded.clone();
    old[..8].copy_from_slice(b"VBDPLAN1");
    assert!(DelegationPlan::decode(&old).is_err());
    let mut legacy = initial_profile(before(false), false);
    assert!(legacy
        .validate_proposal(op(6002), &encoded, std::iter::empty())
        .is_err());
    let mut root_before = t.grant().clone().into_input();
    root_before.parent = None;
    let mut root_after = after.clone().into_input();
    root_after.parent = None;
    let root_before = ResponsibilityManifest::new(root_before).unwrap();
    let root = TransferIntent::move_remaining(
        root_before,
        ResponsibilityManifest::new(root_after).unwrap(),
    )
    .unwrap();
    assert!(root.delegation().is_none());
    assert_eq!(
        TransferIntent::decode(&root.encode(200000).unwrap()).unwrap(),
        root
    );
    assert!(TransferIntent::move_remaining(t.grant().clone(), after.clone()).is_err());
    for group_id in [
        group(21),
        GroupIdentity {
            incarnation: GroupIncarnation::new(2).unwrap(),
            ..group(21)
        },
        t.grant().input().authority,
    ] {
        let mut bad = after.clone().into_input();
        let ExecutionMode::Delegated(routes) = &mut bad.execution else {
            unreachable!()
        };
        routes[2].target = RouteTarget::Group(group_id);
        assert!(make(ResponsibilityManifest::new(bad).unwrap()).is_err());
    }
    for index in [0, 1] {
        let mut bad = after.clone().into_input();
        let ExecutionMode::Delegated(routes) = &mut bad.execution else {
            unreachable!()
        };
        let RouteTarget::Child(mut c) = routes[index].target else {
            unreachable!()
        };
        c.epoch = OwnershipEpoch::new(2).unwrap();
        routes[index].target = RouteTarget::Child(c);
        assert!(make(ResponsibilityManifest::new(bad).unwrap()).is_err());
    }
    let mut bad = after.clone().into_input();
    let ExecutionMode::Delegated(routes) = &mut bad.execution else {
        unreachable!()
    };
    routes[3].target = routes[2].target;
    assert!(make(ResponsibilityManifest::new(bad).unwrap()).is_err());
    let reservation = DelegationReservationStatus {
        operation: op(6002),
        index: 100,
        plan,
    };
    let i = reservation
        .child_intent(ConfigurationId::new(1).unwrap())
        .unwrap();
    let encoded = i.encode(200000).unwrap();
    for end in 0..encoded.len() {
        assert!(TransferIntent::decode(&encoded[..end]).is_err());
    }
    for tag in [b"VBTINT01", b"VBTINT02", b"VBTINT06"] {
        let mut old = encoded.clone();
        old[..8].copy_from_slice(tag);
        assert!(TransferIntent::decode(&old).is_err());
    }
    assert!(legacy
        .validate_proposal(op(3000), &encoded, std::iter::empty())
        .is_err());
    assert!(d.clone().with_remaining_transfer().is_err());
    let cp = d.checkpoint(400000).unwrap();
    assert!(legacy
        .restore_checkpoint(14, d.applied_index(), &cp)
        .is_err());
    let mut fresh = directory(before(false), false);
    for end in 0..cp.len() {
        assert!(fresh
            .restore_checkpoint(14, d.applied_index(), &cp[..end])
            .is_err());
    }
    assert_eq!(fresh.applied_index(), 2);
    let mut target = child(&i, 40);
    let boot = target.bootstrap_command(200000).unwrap();
    commit(&mut target, 3000, boot);
    let cp = target.checkpoint(400000).unwrap();
    for end in 0..cp.len() {
        assert!(child(&i, 40)
            .restore_checkpoint(7, target.applied_index(), &cp[..end])
            .is_err());
    }
    assert!(child(&i, 40)
        .restore_checkpoint(2, target.applied_index(), &cp)
        .is_err());
    t = reopen(&t, &first, 2);
    assert_eq!(read(&t, 100), TargetRead::Data(9));
}

#[cfg(feature = "native")]
#[test]
fn interrupted_remaining_freeze_preserves_only_a_complete_owner_boundary() {
    use support::{Fault, ModelIo};
    use voteboat::{log::*, native::log_store::*};
    let (mut d, mut t, first, mut entries) = initial_with_directory(2, directory);
    let _ = transfer(&mut d, &mut t, &mut entries, 0, range(0, 64));
    let _ = transfer(&mut d, &mut t, &mut entries, 1, range(64, 96));
    let i = prepare_remaining(&mut d, &t, false);
    let freeze = t.freeze_command(&i, 65536, 200000).unwrap();
    record(&mut t, &mut entries, 3000, freeze.clone());
    let expected = t.freeze_status().unwrap().unwrap();
    let image = t.export_target(group(40), 65536).unwrap();
    let boundary = entries.last().unwrap().index;
    let limits = LogLimits::default();
    let seed = || {
        let io = ModelIo::default();
        let mut store = NativeLogStore::create(io.clone(), support::identity(1), limits).unwrap();
        support::append(
            &mut store,
            vec![LogMutation::Create(support::bootstrap(21, 3))],
        );
        let state = store.state(group(21)).unwrap();
        support::append(
            &mut store,
            vec![support::update(
                &state,
                1,
                boundary - 1,
                Some(Suffix {
                    from: 1,
                    entries: entries[..entries.len() - 1].to_vec(),
                }),
            )],
        );
        (io, store)
    };
    let (_, store) = seed();
    let mutation = support::update(
        &store.state(group(21)).unwrap(),
        1,
        boundary,
        Some(Suffix {
            from: boundary,
            entries: vec![entries.last().unwrap().clone()],
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
        let (io, mut store) = seed();
        io.0.borrow_mut().fault = fault;
        if let Ok(tickets) = store.append_batch(vec![mutation.clone()]) {
            assert!(store.barrier(&tickets).is_err());
        }
        drop(store);
        io.0.borrow_mut().power_loss();
        let store = NativeLogStore::recover(io, support::identity(1), limits).unwrap();
        let state = store.state(group(21)).unwrap();
        let mut app = selected(&first, 2);
        app.apply_batch(
            &state
                .entries
                .into_iter()
                .filter(|e| e.index <= state.commit_index)
                .collect::<Vec<_>>(),
        )
        .unwrap();
        if state.commit_index == boundary {
            complete = true;
            assert!(matches!(read(&app, 100), TargetRead::Rejected(_)));
        } else {
            old = true;
            assert_eq!(state.commit_index, boundary - 1);
            assert_eq!(read(&app, 100), TargetRead::Data(9));
        }
        app = reopen(&app, &first, 2);
        assert!(matches!(
            commit(&mut app, 3000, freeze.clone()).outcome,
            TargetOutcome::Frozen(_)
        ));
        assert_eq!(app.freeze_status().unwrap(), Some(expected.clone()));
        assert_eq!(app.export_target(group(40), 65536).unwrap(), image);
    }
    assert!(old && complete);
}

#[path = "remaining/retirement.rs"]
mod retirement;
