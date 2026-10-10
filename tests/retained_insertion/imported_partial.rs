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
fn route_scope(t: &Target, key: u8) -> BucketRange {
    match &t.grant().input().execution {
        ExecutionMode::Single(_) => t.grant().input().scope,
        ExecutionMode::Delegated(routes) => {
            routes
                .iter()
                .find(|r| r.scope.contains(key.into()))
                .unwrap()
                .scope
        }
        _ => panic!("owner routes"),
    }
}
fn record(
    t: &mut Target,
    log: &mut Vec<voteboat::log::LogEntry>,
    id: u128,
    bytes: Vec<u8>,
) -> TargetOutcome<BucketReceipt> {
    let e = entry(t.applied_index() + 1, id, bytes);
    let r = t.apply_batch(std::slice::from_ref(&e)).unwrap().remove(0);
    log.push(e);
    r.outcome
}
fn selected(i: &TransferIntent, limit: usize) -> Target {
    target(i)
        .with_parent_adoption(4)
        .unwrap_or_else(|_| panic!("parent"))
        .with_partial_delegation(limit, 65536)
        .unwrap_or_else(|_| panic!("partial"))
}
fn read(t: &Target, key: u8) -> TargetRead<i64> {
    let m = t.grant().input();
    let h = RouteHint {
        responsibility: m.responsibility,
        group: group(21),
        application: m.application,
        scheme: m.scheme,
        scope: route_scope(t, key),
        bucket: key.into(),
        epoch: m.epoch,
        generation: m.generation,
    };
    t.read_at(
        t.applied_index(),
        TargetQuery::Data(RoutedQuery {
            hint: h,
            key: vec![key],
            query: vec![key],
        }),
    )
    .unwrap()
}
fn command(t: &Target, key: u8, delta: i64) -> Vec<u8> {
    let m = t.grant().input();
    let h = RouteHint {
        responsibility: m.responsibility,
        group: group(21),
        application: m.application,
        scheme: m.scheme,
        scope: route_scope(t, key),
        bucket: key.into(),
        epoch: m.epoch,
        generation: m.generation,
    };
    encode_routed(
        h,
        &[key],
        &encode_add(&[key], delta, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap()
}
fn initial(
    limit: usize,
) -> (
    Directory,
    Target,
    TransferIntent,
    Vec<voteboat::log::LogEntry>,
) {
    initial_with_directory(limit, initial_profile)
}
fn initial_with_directory(
    limit: usize,
    profile: fn(ResponsibilityManifest, bool) -> Directory,
) -> (
    Directory,
    Target,
    TransferIntent,
    Vec<voteboat::log::LogEntry>,
) {
    let (mut d, _, i) = setup_with_directory(false, false, profile);
    let mut s = retained_source(&i);
    let boot = s.bootstrap_command(200000).unwrap();
    commit(&mut s, 100, boot);
    commit(&mut s, 1, data_at(&i, 1, 7));
    commit(&mut s, 2, data_at(&i, 100, 9));
    commit(&mut d, 200, i.encode(200000).unwrap());
    commit(&mut s, 200, i.encode(200000).unwrap());
    let ScopedSourceRead::Frozen(Some(frozen)) = s
        .read_at(s.applied_index(), ScopedSourceQuery::Frozen(op(200)))
        .unwrap()
    else {
        panic!("frozen")
    };
    let cfg = ConfigurationId::new(1).unwrap();
    let mut t = selected(&i, limit);
    let mut log = Vec::new();
    let boot = t.bootstrap_command(200000).unwrap();
    record(&mut t, &mut log, 200, boot);
    let import = TargetImport::new(
        op(200),
        i.clone(),
        group(21),
        vec![SourceImport {
            fence: frozen.fence.fence,
            configuration: cfg,
            image: s.export(op(200), 65536).unwrap(),
            digest: frozen.digest,
        }],
    )
    .unwrap();
    let bytes = t.import_command(&import, 200000).unwrap();
    record(&mut t, &mut log, 200, bytes);
    assert_eq!(read(&t, 1), TargetRead::NotActive);
    let pubn = TransferPublication::new(
        op(200),
        i.clone(),
        vec![SourceFenceEvidence::from_scoped_status(cfg, frozen, &i).unwrap()],
        vec![TargetReadyEvidence::from_status(cfg, t.status()).unwrap()],
    )
    .unwrap();
    assert!(matches!(
        commit(&mut d, 201, pubn.encode(200000).unwrap()).outcome,
        DirectoryOutcome::TransferPublished(_)
    ));
    let decision = d
        .transfer_publication_at(d.applied_index(), op(200))
        .unwrap()
        .unwrap();
    let bytes = t
        .activation_command(
            &TargetActivation {
                metadata_configuration: cfg,
                decision,
            },
            200000,
        )
        .unwrap();
    assert!(matches!(
        record(&mut t, &mut log, 200, bytes),
        TargetOutcome::Activated(_)
    ));
    assert_eq!(read(&t, 1), TargetRead::Data(7));
    assert_eq!(read(&t, 100), TargetRead::Data(9));
    (d, t, i, log)
}
fn reopen(t: &Target, i: &TransferIntent, limit: usize) -> Target {
    let mut fresh = selected(i, limit);
    fresh
        .restore_checkpoint(6, t.applied_index(), &t.checkpoint(400000).unwrap())
        .unwrap();
    assert_eq!(fresh.grant(), t.grant());
    assert_eq!(fresh.status(), t.status());
    fresh
}
fn prepare(
    d: &mut Directory,
    t: &Target,
    cycle: u128,
    scope: BucketRange,
) -> (TransferIntent, DelegationReservationStatus) {
    let before = t.grant().clone();
    let id = 30 + cycle;
    let transfer = 2000 + cycle;
    let reserve = 6000 + cycle * 10;
    let mut c = before.clone().into_input();
    c.responsibility = rid(id);
    c.parent = Some(ParentAuthority {
        responsibility: before.input().responsibility,
        group: before.input().authority,
    });
    c.scope = scope;
    c.epoch = OwnershipEpoch::new(1).unwrap();
    c.generation = RouteGeneration::new(1).unwrap();
    c.execution = ExecutionMode::Single(group(id));
    let c = ResponsibilityManifest::new(c).unwrap();
    let creation = GroupCreationIntent {
        authority: before.input().authority,
        parent: before.input().responsibility,
        expected: before.input().generation,
        responsibility: c.input().responsibility,
        bootstrap: support::bootstrap(id, 3),
        application: before.input().application,
        mode: GroupCreationMode::Staging,
    };
    assert_eq!(
        commit(d, id, creation.encode(200000).unwrap()).outcome,
        DirectoryOutcome::CreationReserved
    );
    let created = d
        .group_creation_at(d.applied_index(), group(id))
        .unwrap()
        .unwrap();
    let mut after = before.clone().into_input();
    after.epoch = OwnershipEpoch::new(after.epoch.get() + 1).unwrap();
    after.generation = RouteGeneration::new(after.generation.get() + 1).unwrap();
    let old = match &before.input().execution {
        ExecutionMode::Single(g) => vec![RouteEntry {
            scope: before.input().scope,
            target: RouteTarget::Group(*g),
        }],
        ExecutionMode::Delegated(v) => v.clone(),
        _ => panic!("owner"),
    };
    let mut routes = Vec::new();
    for r in old {
        if r.scope.start() <= scope.start()
            && r.scope.end() >= scope.end()
            && r.target == RouteTarget::Group(group(21))
        {
            if r.scope.start() < scope.start() {
                routes.push(RouteEntry {
                    scope: range(r.scope.start(), scope.start()),
                    target: r.target,
                });
            }
            routes.push(RouteEntry {
                scope,
                target: RouteTarget::Child(ChildAuthority {
                    responsibility: c.input().responsibility,
                    group: c.input().authority,
                    epoch: c.input().epoch,
                }),
            });
            if scope.end() < r.scope.end() {
                routes.push(RouteEntry {
                    scope: range(scope.end(), r.scope.end()),
                    target: r.target,
                });
            }
        } else {
            routes.push(r);
        }
    }
    after.execution = ExecutionMode::Delegated(routes);
    let after = ResponsibilityManifest::new(after).unwrap();
    let plan = DelegationPlan::retained_insertion(
        d.manifest(rid(10)).unwrap().clone(),
        before,
        after,
        InsertionChild::from_creation(c, &created).unwrap(),
        op(transfer),
    )
    .unwrap();
    assert_eq!(
        commit(d, reserve, plan.encode(200000).unwrap()).outcome,
        DirectoryOutcome::DelegationReserved
    );
    let reservation = d
        .delegation_reservation_at(d.applied_index(), op(reserve))
        .unwrap()
        .unwrap();
    let cfg = ConfigurationId::new(1).unwrap();
    let i = reservation.child_intent(cfg).unwrap();
    assert_eq!(
        commit(d, transfer, i.encode(200000).unwrap()).outcome,
        DirectoryOutcome::TransferIntentRecorded
    );
    (i, reservation)
}
fn transfer(
    d: &mut Directory,
    t: &mut Target,
    log: &mut Vec<voteboat::log::LogEntry>,
    cycle: u128,
    scope: BucketRange,
) -> (Target, TransferIntent, Vec<u8>) {
    let (i, reservation) = prepare(d, t, cycle, scope);
    let id = 30 + cycle;
    let transfer = 2000 + cycle;
    let reserve = 6000 + cycle * 10;
    let cfg = ConfigurationId::new(1).unwrap();
    assert!(matches!(
        record(t, log, transfer, i.encode(200000).unwrap()),
        TargetOutcome::ScopeFenced(_)
    ));
    assert_eq!(read(t, 100), TargetRead::Data(9));
    assert!(matches!(
        read(t, scope.start() as u8),
        TargetRead::Rejected(_)
    ));
    let status = t.scoped_freeze(op(transfer)).unwrap();
    let mut child = Target::new(
        group(id),
        op(transfer),
        i.clone(),
        BucketCounter::new(scope, Policy, bucket_limits()).unwrap(),
        Policy,
        TargetLimits {
            import_bytes: 32768,
            application_checkpoint_bytes: bucket_limits().checkpoint_bound().unwrap(),
        },
    )
    .unwrap_or_else(|_| panic!("child"));
    let b = child.bootstrap_command(200000).unwrap();
    commit(&mut child, transfer, b);
    let import = TargetImport::new(
        op(transfer),
        i.clone(),
        group(id),
        vec![SourceImport {
            fence: status.fence.fence,
            configuration: cfg,
            image: t.export_scoped(op(transfer), 65536).unwrap(),
            digest: status.digest,
        }],
    )
    .unwrap();
    let b = child.import_command(&import, 200000).unwrap();
    commit(&mut child, transfer, b);
    let publication = TransferPublication::new(
        op(transfer),
        i.clone(),
        vec![SourceFenceEvidence::from_scoped_status(cfg, status, &i).unwrap()],
        vec![TargetReadyEvidence::from_status(cfg, child.status()).unwrap()],
    )
    .unwrap();
    assert!(matches!(
        commit(d, transfer + 100, publication.encode(200000).unwrap()).outcome,
        DirectoryOutcome::TransferPublished(_)
    ));
    let decision = d
        .transfer_publication_at(d.applied_index(), op(transfer))
        .unwrap()
        .unwrap();
    let completion = DelegationCompletion {
        reservation: op(reserve),
        reservation_index: reservation.index,
        parent_configuration: cfg,
        child_configuration: cfg,
        decision: decision.clone(),
    };
    assert!(matches!(
        commit(d, reserve + 1, completion.encode(200000).unwrap()).outcome,
        DirectoryOutcome::DelegationPublished(_)
    ));
    let b = child
        .activation_command(
            &TargetActivation {
                metadata_configuration: cfg,
                decision: decision.clone(),
            },
            200000,
        )
        .unwrap();
    commit(&mut child, transfer, b);
    let adoption = RetainedGrantAdoption {
        metadata_configuration: cfg,
        decision,
    }
    .encode(200000)
    .unwrap();
    assert!(matches!(
        record(t, log, 7000 + cycle, adoption.clone()),
        TargetOutcome::GrantAdopted(_)
    ));
    assert_eq!(read(t, 100), TargetRead::Data(9));
    (child, i, adoption)
}
#[test]
fn activated_import_delegates_twice_keeps_retries_and_recovers_mixed_history() {
    let (mut d, mut t, first, mut log) = initial(2);
    let original = t.status();
    let (mut child, i, adoption) = transfer(&mut d, &mut t, &mut log, 0, range(0, 64));
    assert_eq!(child.application().value(&[1]), Ok(7));
    let mut h = source_hint(&first, 1);
    let m = i.target_manifest(group(30)).unwrap().input();
    h.responsibility = m.responsibility;
    h.group = group(30);
    h.scope = m.scope;
    h.epoch = m.epoch;
    h.generation = m.generation;
    let r = commit(
        &mut child,
        1,
        encode_routed(
            h,
            &[1],
            &encode_add(&[1], 7, b"effect", 1024).unwrap(),
            4096,
        )
        .unwrap(),
    );
    assert!(matches!(r.outcome,TargetOutcome::Applied(r) if r.duplicate));
    let image = t.export_scoped(op(2000), 65536).unwrap();
    t = reopen(&t, &first, 2);
    assert!(matches!(
        record(&mut t, &mut log, 7000, adoption),
        TargetOutcome::GrantAdopted(_)
    ));
    let (_, _, _) = transfer(&mut d, &mut t, &mut log, 1, range(64, 96));
    assert_eq!(t.export_scoped(op(2000), 65536).unwrap(), image);
    let b = command(&t, 100, 9);
    assert!(matches!(record(&mut t,&mut log,2,b),TargetOutcome::Applied(r) if r.duplicate));
    let b = command(&t, 100, 3);
    record(&mut t, &mut log, 8, b);
    assert_eq!(read(&t, 100), TargetRead::Data(12));
    t = reopen(&t, &first, 2);
    assert_eq!(t.status(), original);
    assert_eq!(read(&t, 100), TargetRead::Data(12));
    for e in &log[..3] {
        assert!(matches!(
            commit(
                &mut t,
                200,
                match &e.payload {
                    voteboat::log::EntryPayload::Command { bytes, .. } => bytes.clone(),
                    _ => unreachable!(),
                }
            )
            .outcome,
            TargetOutcome::Staged { .. }
                | TargetOutcome::Imported { .. }
                | TargetOutcome::Activated(_)
        ));
    }
    let mut replay = selected(&first, 2);
    replay.apply_batch(&log).unwrap();
    assert_eq!(replay.grant(), t.grant());
    assert_eq!(read(&replay, 100), TargetRead::Data(12));
}

#[test]
fn partial_target_rejects_unactivated_conflicting_pending_and_corrupt_histories() {
    let (mut d, mut t, first, mut log) = initial(1);
    let before = t.clone();
    let (_, _, adoption) = transfer(&mut d, &mut t, &mut log, 0, range(0, 64));
    let voteboat::log::EntryPayload::Command { bytes: freeze, .. } = &log[3].payload else {
        unreachable!()
    };
    assert!(before
        .validate_proposal(op(7000), &adoption, std::iter::empty())
        .is_err());
    assert!(before
        .validate_proposal(
            op(7000),
            &adoption,
            [(op(2000), freeze.as_slice())].into_iter()
        )
        .is_ok());
    assert!(selected(&first, 1)
        .validate_proposal(op(2000), freeze, std::iter::empty())
        .is_err());
    assert!(target(&first)
        .validate_proposal(op(2000), freeze, std::iter::empty())
        .is_err());
    let snapshot = t.checkpoint(400000).unwrap();
    let mut changed = adoption.clone();
    changed[8] ^= 1;
    assert!(t
        .validate_proposal(op(7000), &changed, std::iter::empty())
        .is_err());
    assert!(t
        .validate_proposal(op(30), &command(&t, 100, 1), std::iter::empty())
        .is_err());
    assert_eq!(t.checkpoint(400000).unwrap(), snapshot);
    let write = command(&t, 100, 1);
    assert!(t
        .apply_batch(&[
            entry(t.applied_index() + 1, 8000, write),
            entry(t.applied_index() + 2, 7000, changed),
        ])
        .is_err());
    assert_eq!(t.checkpoint(400000).unwrap(), snapshot);
    for end in 0..snapshot.len() {
        let mut fresh = selected(&first, 1);
        assert!(fresh
            .restore_checkpoint(6, t.applied_index(), &snapshot[..end])
            .is_err());
        assert_eq!(fresh.applied_index(), 0);
    }
    let mut bad = snapshot.clone();
    *bad.last_mut().unwrap() ^= 1;
    assert!(selected(&first, 1)
        .restore_checkpoint(6, t.applied_index(), &bad)
        .is_err());
    assert!(selected(&first, 2)
        .restore_checkpoint(6, t.applied_index(), &snapshot)
        .is_err());
    for maximum in [0, 17, usize::MAX] {
        assert!(target(&first)
            .with_partial_delegation(maximum, 65536)
            .is_err());
    }
    assert!(target(&first)
        .with_partial_delegation(16, voteboat::scope::MAX_SCOPE_IMAGE_BYTES)
        .is_err());
    assert!(t.clone().with_parent_adoption(1).is_err());
    assert!(t.clone().with_partial_delegation(1, 65536).is_err());
    assert_eq!(read(&t, 100), TargetRead::Data(9));
    let (next, _) = prepare(&mut d, &t, 1, range(64, 96));
    let before = t.checkpoint(400000).unwrap();
    assert_eq!(
        t.validate_proposal(op(2001), &next.encode(200000).unwrap(), std::iter::empty()),
        Err(ApplicationError::ReceiptBudget)
    );
    assert_eq!(t.checkpoint(400000).unwrap(), before);
}

#[test]
fn full_imported_data_history_keeps_partial_control_capacity() {
    let (mut d, mut t, first, mut log) = initial(2);
    for id in 10000..10030 {
        let bytes = command(&t, 100, 0);
        assert!(matches!(
            record(&mut t, &mut log, id, bytes),
            TargetOutcome::Applied(_)
        ));
    }
    assert_eq!(t.application().outbox().count(), 32);
    let bytes = command(&t, 100, 1);
    assert!(t
        .validate_proposal(op(20000), &bytes, std::iter::empty())
        .is_err());
    let _ = transfer(&mut d, &mut t, &mut log, 0, range(0, 64));
    assert!(t.retained_grant(op(7000)).is_some());
    t = reopen(&t, &first, 2);
    assert_eq!(read(&t, 100), TargetRead::Data(9));
    assert_eq!(t.application().outbox().count(), 32);
}

#[test]
fn partial_history_interleaves_parent_and_slot_changes_without_losing_import_lineage() {
    use voteboat::reparenting::*;
    let (mut d, mut t, first, mut log) = initial(2);
    let (_, _, adoption) = transfer(&mut d, &mut t, &mut log, 0, range(0, 64));
    let image = t.export_scoped(op(2000), 65536).unwrap();
    let original = t.status();
    let mut next = d.manifest(rid(10)).unwrap().clone().into_input();
    next.responsibility = rid(600);
    next.generation = RouteGeneration::new(1).unwrap();
    let ExecutionMode::Delegated(routes) = &mut next.execution else {
        panic!("parent")
    };
    routes[0].target = RouteTarget::Vacant;
    let next = ResponsibilityManifest::new(next).unwrap();
    // Supplied original local quorum observations; this case checks the data
    // owner ledger. The preceding transfer executes the actual Directory path.
    let plan = ReparentPlan::new(
        d.manifest(rid(10)).unwrap().clone(),
        next.clone(),
        t.grant().clone(),
    )
    .unwrap();
    let bytes = OwnerParentAdoption {
        metadata_configuration: ConfigurationId::new(1).unwrap(),
        decision: ReparentStatus {
            operation: op(8000),
            index: 50,
            plan,
        },
    }
    .encode(200000)
    .unwrap();
    assert!(matches!(
        record(&mut t, &mut log, 9000, bytes.clone()),
        TargetOutcome::ParentAdopted(_)
    ));
    let mut destination = next.into_input();
    destination.responsibility = rid(700);
    destination.execution = ExecutionMode::Delegated(vec![
        RouteEntry {
            scope: range(0, 64),
            target: RouteTarget::Vacant,
        },
        RouteEntry {
            scope: range(64, 256),
            target: RouteTarget::Group(group(40)),
        },
    ]);
    let destination = ResponsibilityManifest::new(destination).unwrap();
    let plan = ReparentPlan::new(
        t.grant().clone(),
        destination,
        d.manifest(rid(30)).unwrap().clone(),
    )
    .unwrap();
    let slots = ParentSlotAdoption {
        side: ReparentSide::Old,
        observation: OwnerParentAdoption {
            metadata_configuration: ConfigurationId::new(1).unwrap(),
            decision: ReparentStatus {
                operation: op(8001),
                index: 51,
                plan,
            },
        },
    }
    .encode(200000)
    .unwrap();
    assert!(matches!(
        record(&mut t, &mut log, 9001, slots),
        TargetOutcome::ParentAdopted(_)
    ));
    t = reopen(&t, &first, 2);
    assert_eq!(t.status(), original);
    assert_eq!(t.export_scoped(op(2000), 65536).unwrap(), image);
    assert!(matches!(
        record(&mut t, &mut log, 9000, bytes),
        TargetOutcome::ParentAdopted(_)
    ));
    assert!(matches!(
        record(&mut t, &mut log, 7000, adoption),
        TargetOutcome::GrantAdopted(_)
    ));
    assert_eq!(read(&t, 100), TargetRead::Data(9));
    use voteboat::retirement::RetirableOwner;
    let lineage = t.retirement_lineage().unwrap();
    selected(&first, 2)
        .validate_retirement_lineage(&lineage, t.applied_index() + 1)
        .unwrap();
}

#[cfg(feature = "native")]
#[test]
fn partial_freeze_and_grant_frames_recover_exact_images_and_retained_service() {
    use support::{Fault, ModelIo};
    use voteboat::{log::*, native::log_store::*};
    let (mut d, mut t, first, mut entries) = initial(2);
    let (_, _, adoption) = transfer(&mut d, &mut t, &mut entries, 0, range(0, 64));
    let image = t.export_scoped(op(2000), 65536).unwrap();
    let limits = LogLimits::default();
    for boundary in [4u64, 5] {
        let seed = || {
            let io = ModelIo::default();
            let mut store =
                NativeLogStore::create(io.clone(), support::identity(1), limits).unwrap();
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
                        entries: entries[..boundary as usize - 1].to_vec(),
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
            } else {
                old = true;
                assert_eq!(state.commit_index, boundary - 1);
            }
            assert_eq!(read(&app, 100), TargetRead::Data(9));
            if state.commit_index >= 4 {
                assert_eq!(app.export_scoped(op(2000), 65536).unwrap(), image);
                assert!(matches!(read(&app, 1), TargetRead::Rejected(_)));
            } else {
                assert_eq!(read(&app, 1), TargetRead::Data(7));
            }
            app = reopen(&app, &first, 2);
            let EntryPayload::Command { bytes, .. } = &entries[3].payload else {
                unreachable!()
            };
            assert!(matches!(
                commit(&mut app, 2000, bytes.clone()).outcome,
                TargetOutcome::ScopeFenced(_)
            ));
            assert!(matches!(
                commit(&mut app, 7000, adoption.clone()).outcome,
                TargetOutcome::GrantAdopted(_)
            ));
            assert_eq!(app.export_scoped(op(2000), 65536).unwrap(), image);
            assert_eq!(read(&app, 100), TargetRead::Data(9));
        }
        assert!(old && complete);
    }
}

#[path = "imported_partial/remaining.rs"]
mod remaining;
