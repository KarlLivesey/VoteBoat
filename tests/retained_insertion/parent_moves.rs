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
use voteboat::{reparent_commit::*, reparent_guard::*};
fn moved() -> (
    Directory,
    Directory,
    Source,
    TransferIntent,
    RetainedGrantAdoption,
    CrossOwnerParentAdoption,
) {
    let (mut d, p, s, i, a) = handoff_profile(true, true);
    let mut p = p.unwrap();
    let child = d
        .manifest(i.before().input().responsibility)
        .unwrap()
        .clone();
    let mut new = p.manifest(rid(500)).unwrap().clone().into_input();
    new.responsibility = rid(600);
    new.authority = group(200);
    new.generation = RouteGeneration::new(1).unwrap();
    new.execution = ExecutionMode::Delegated(vec![RouteEntry {
        scope: new.scope,
        target: RouteTarget::Vacant,
    }]);
    let new = ResponsibilityManifest::new(new).unwrap();
    let mut destination = move_directory(vec![new.clone()]);
    let b = destination.bootstrap_command(200000).unwrap();
    commit(&mut destination, 1000, b);
    commit(
        &mut destination,
        1001,
        DirectoryCommand {
            expected: None,
            manifest: new.clone(),
        }
        .encode(200000)
        .unwrap(),
    );
    let plan = CrossReparentPlan::new(
        rid(500),
        rid(600),
        child.input().responsibility,
        vec![
            p.manifest(rid(500)).unwrap().clone(),
            new,
            child,
            d.manifest(rid(21)).unwrap().clone(),
        ],
    )
    .unwrap();
    let cfg = prepare_parent_move(&mut d, &mut p, &mut destination, &plan);
    let guards = [&d, &p, &destination]
        .into_iter()
        .map(|n| {
            ReparentGuardEvidence::from_status(
                cfg,
                &n.reparent_guard_at(n.applied_index(), op(4000))
                    .unwrap()
                    .unwrap(),
            )
            .unwrap()
        })
        .collect();
    assert_eq!(
        commit(
            &mut d,
            4001,
            CommitReparent::new(op(4000), guards)
                .unwrap()
                .encode(MAX_REPARENT_COMPLETION_BYTES)
                .unwrap()
        )
        .outcome,
        DirectoryOutcome::ReparentCommitted
    );
    let decision = PublishReparent {
        configuration: cfg,
        decision: d
            .reparent_decision_at(d.applied_index(), op(4000))
            .unwrap()
            .unwrap(),
    };
    for n in [&mut p, &mut destination] {
        assert_eq!(
            commit(
                n,
                4002,
                decision.encode(MAX_REPARENT_COMPLETION_BYTES).unwrap()
            )
            .outcome,
            DirectoryOutcome::ReparentPublished
        );
    }
    let completion = finish_parent_move(&mut d, &mut p, &mut destination, cfg);
    let adoption = CrossOwnerParentAdoption {
        plan,
        decision,
        child_configuration: cfg,
        child_publication: d
            .reparent_publication_at(d.applied_index(), op(4000))
            .unwrap()
            .unwrap(),
        completion,
    };
    (d, destination, s, i, a, adoption)
}
fn reopen(s: &Source, i: &TransferIntent) -> Source {
    let mut n = source_profile(i, true);
    n.restore_checkpoint(4, s.applied_index(), &s.checkpoint(200000).unwrap())
        .unwrap();
    n
}
#[test]
fn completed_cross_parent_move_preserves_scoped_exports_and_next_retained_handoff() {
    let (mut d, mut p, mut s, first, original, adoption) = moved();
    let first_image = s.export(op(200), 65536).unwrap();
    let b = adoption.encode(MAX_CROSS_PARENT_ADOPTION_BYTES).unwrap();
    let adopted = commit(&mut s, 9000, b.clone()).outcome;
    assert!(matches!(adopted, RoutedOutcome::ParentAdopted(_)));
    assert_eq!(s.grant(), &adoption.after());
    s = reopen(&s, &first);
    assert_eq!(s.export(op(200), 65536).unwrap(), first_image);
    let before = s.grant().clone();
    let (child, after, intent) = reserve_moved_child(&mut d, &mut p, &before);
    let mut hint = source_hint(&first, 150);
    hint.epoch = before.input().epoch;
    hint.generation = before.input().generation;
    hint.scope = range(128, 256);
    let write = encode_routed(
        hint,
        &[150],
        &encode_add(&[150], 5, b"second", 1024).unwrap(),
        4096,
    )
    .unwrap();
    commit(&mut s, 6, write);
    let old = s.export(op(200), 65536).unwrap();
    let fenced = commit(&mut s, 202, intent.encode(100000).unwrap());
    assert!(
        matches!(fenced.outcome,RoutedOutcome::ScopeFenced(f) if f.fence.epoch==before.input().epoch)
    );
    let ScopedSourceRead::Frozen(Some(status)) = s
        .read_at(s.applied_index(), ScopedSourceQuery::Frozen(op(202)))
        .unwrap()
    else {
        panic!("frozen")
    };
    let (mut target, decision) = activate_moved_child(&mut d, &mut p, &s, &intent, &after, status);
    let adoption = RetainedGrantAdoption {
        metadata_configuration: ConfigurationId::new(1).unwrap(),
        decision,
    };
    let bytes = adoption.encode(100000).unwrap();
    commit(&mut s, 301, bytes.clone());
    assert_eq!(s.grant(), &after);
    assert_eq!(s.export(op(200), 65536).unwrap(), old);
    let cp = s.checkpoint(100000).unwrap();
    let mut reopened = source_profile(&first, true);
    reopened
        .restore_checkpoint(4, s.applied_index(), &cp)
        .unwrap();
    s = reopened;
    assert!(
        matches!(commit(&mut s,301,bytes).outcome,RoutedOutcome::GrantAdopted(f) if f.epoch==after.input().epoch)
    );
    check_remaining_parent_data(&mut s, &first, &after);
    assert_eq!(s.export(op(200), 65536).unwrap(), old);
    check_moved_child_retry(&mut target, &child);
    assert_eq!(commit(&mut s, 9000, b.clone()).outcome, adopted);
    assert!(matches!(
        commit(&mut s, 300, original.encode(200000).unwrap()).outcome,
        RoutedOutcome::GrantAdopted(_)
    ));
    assert_eq!(s.export(op(200), 65536).unwrap(), first_image);
    commit(&mut s, 999, encode_fence(after.input().epoch));
    assert_eq!(s.fence().unwrap().epoch, after.input().epoch);
    assert_eq!(commit(&mut s, 9000, b).outcome, adopted);
    let cp = s.checkpoint(100000).unwrap();
    let mut reopened = source_profile(&first, true);
    reopened
        .restore_checkpoint(4, s.applied_index(), &cp)
        .unwrap();
    assert_eq!(reopened.fence(), s.fence());
    assert_eq!(reopened.export(op(200), 65536).unwrap(), old);
}

#[test]
fn parent_profile_replay_operation_reserve_and_malformed_mixed_history_are_atomic() {
    let (_, _, mut s, i, _, a) = moved();
    let b = a.encode(200000).unwrap();
    assert!(source(&i).with_parent_adoption(1).is_err());
    for limit in [0, usize::MAX, 64] {
        assert!(retained_source(&i).with_parent_adoption(limit).is_err());
    }
    assert!(source_profile(&i, true).with_parent_adoption(1).is_err());
    let cp = s.checkpoint(200000).unwrap();
    for id in [100, 1, 2, 3, 4, 200, 300, 21] {
        assert!(s.validate_proposal(op(id), &b, std::iter::empty()).is_err());
        assert_eq!(s.checkpoint(200000).unwrap(), cp);
    }
    assert!(s
        .validate_proposal(op(9000), &b, [(op(9000), b.as_slice())].into_iter())
        .is_ok());
    assert!(s
        .validate_proposal(op(9001), &b, [(op(9000), b.as_slice())].into_iter())
        .is_err());
    assert!(s
        .apply_batch(&[
            entry(s.applied_index() + 1, 9000, b.clone()),
            entry(s.applied_index() + 2, 9100, vec![0])
        ])
        .is_err());
    assert_eq!(s.checkpoint(200000).unwrap(), cp);
    let RoutedOutcome::ParentAdopted(status) = commit(&mut s, 9000, b.clone()).outcome else {
        panic!("parent")
    };
    let q = ScopedSourceQuery::ParentAdoption(op(9000));
    let read = s.read_at(s.applied_index(), q.clone()).unwrap();
    assert_eq!(read, ScopedSourceRead::ParentAdoption(Some(status)));
    assert_eq!(s.query_bytes(&q, 0).unwrap(), 0);
    assert_eq!(s.read_result_bytes(&read, 0).unwrap(), 0);
    let mut old = retained_source(&i);
    let boot = old.bootstrap_command(200000).unwrap();
    commit(&mut old, 100, boot);
    assert!(old
        .validate_proposal(op(9000), &b, std::iter::empty())
        .is_err());
    let cp = s.checkpoint(200000).unwrap();
    assert!(old.restore_checkpoint(4, s.applied_index(), &cp).is_err());
    let mut fresh = source_profile(&i, true);
    let pristine = fresh.checkpoint(200000).unwrap();
    for end in 0..cp.len() {
        assert!(fresh
            .restore_checkpoint(4, s.applied_index(), &cp[..end])
            .is_err());
        assert_eq!(fresh.checkpoint(200000).unwrap(), pristine);
    }
    let record = cp.len() - b.len() - 61;
    for (offset, change) in [
        (record, vec![0]),
        (record + 1, vec![0; 32]),
        (record + 33, op(300).get().to_le_bytes().to_vec()),
        (record + 49, 1u64.to_le_bytes().to_vec()),
    ] {
        let mut bad = cp.clone();
        bad[offset..offset + change.len()].copy_from_slice(&change);
        assert!(fresh
            .restore_checkpoint(4, s.applied_index(), &bad)
            .is_err());
        assert_eq!(fresh.checkpoint(200000).unwrap(), pristine);
    }
    fresh.restore_checkpoint(4, s.applied_index(), &cp).unwrap();
    assert_eq!(
        fresh.export(op(200), 65536).unwrap(),
        s.export(op(200), 65536).unwrap()
    );
    let mut changed = a;
    changed.child_configuration = ConfigurationId::new(2).unwrap();
    assert!(fresh
        .validate_proposal(
            op(9000),
            &changed.encode(200000).unwrap(),
            std::iter::empty()
        )
        .is_err());
}

#[cfg(feature = "native")]
#[test]
fn mixed_retained_and_parent_journal_cuts_preserve_exports_and_later_full_fence() {
    use support::Fault;
    use voteboat::{log::*, native::log_store::*};
    let (_, _, _, intent, retained, parent) = moved();
    let prototype = source_profile(&intent, true);
    let adoption = parent.encode(200000).unwrap();
    let entries = [
        entry(1, 100, prototype.bootstrap_command(200000).unwrap()),
        entry(2, 1, data_at(&intent, 1, 7)),
        entry(3, 2, data_at(&intent, 200, 11)),
        entry(4, 200, intent.encode(200000).unwrap()),
        entry(5, 3, data_at(&intent, 200, 2)),
        entry(6, 300, retained.encode(200000).unwrap()),
        entry(7, 9000, adoption.clone()),
        entry(8, 999, encode_fence(parent.after().input().epoch)),
    ];
    let limits = LogLimits::default();
    for boundary in [7u64, 8] {
        let seed = || seed_moved_parent(&entries, boundary, limits);
        let (_, log) = seed();
        let mutation = support::update(
            &log.state(group(20)).unwrap(),
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
            let log = NativeLogStore::recover(io, support::identity(1), limits).unwrap();
            let state = log.state(group(20)).unwrap();
            let mut app = source_profile(&intent, true);
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
            let expected = if state.commit_index >= 7 {
                parent.after()
            } else {
                intent.after().clone()
            };
            assert_eq!(app.grant(), &expected);
            assert_eq!(app.fence().is_some(), state.commit_index == 8);
            let image = app.export(op(200), 65536).unwrap();
            let before = app.parent_adoption(op(9000));
            app = reopen(&app, &intent);
            assert_eq!(app.export(op(200), 65536).unwrap(), image);
            let RoutedOutcome::ParentAdopted(status) =
                commit(&mut app, 9000, adoption.clone()).outcome
            else {
                panic!("parent retry")
            };
            assert_eq!(
                status.index,
                before.map_or(state.commit_index + 1, |p| p.index)
            );
            assert_eq!(app.export(op(200), 65536).unwrap(), image);
            assert_eq!(app.routed().application().outbox().count(), 3);
        }
        assert!(old && complete);
    }
}

#[test]
fn local_parent_adoption_has_independent_capacity_and_preserves_original_status() {
    use voteboat::reparenting::*;
    let mut input = before(false).into_input();
    input.parent = Some(ParentAuthority {
        responsibility: rid(500),
        group: group(1),
    });
    let child = ResponsibilityManifest::new(input).unwrap();
    let mut old = parent(&child).into_input();
    old.authority = group(1);
    let old = ResponsibilityManifest::new(old).unwrap();
    let mut new = old.clone().into_input();
    new.responsibility = rid(600);
    new.execution = ExecutionMode::Delegated(vec![RouteEntry {
        scope: new.scope,
        target: RouteTarget::Vacant,
    }]);
    let new = ResponsibilityManifest::new(new).unwrap();
    let plan = ReparentPlan::new(old.clone(), new.clone(), child.clone()).unwrap();
    let mut d = move_directory(vec![old, new, child.clone()]);
    let boot = d.bootstrap_command(200000).unwrap();
    commit(&mut d, 1000, boot);
    // Publish roots before the child whose parent authority must already exist.
    let mut manifests = d.plan().manifests().cloned().collect::<Vec<_>>();
    manifests.sort_by_key(|m| m.input().parent.is_some());
    for (i, m) in manifests.into_iter().enumerate() {
        let result = commit(
            &mut d,
            1001 + i as u128,
            DirectoryCommand {
                expected: None,
                manifest: m,
            }
            .encode(200000)
            .unwrap(),
        );
        assert!(matches!(result.outcome, DirectoryOutcome::Published(_)));
    }
    assert_eq!(
        commit(&mut d, 400, plan.encode(200000).unwrap()).outcome,
        DirectoryOutcome::Reparented
    );
    let command = OwnerParentAdoption {
        metadata_configuration: ConfigurationId::new(1).unwrap(),
        decision: d
            .reparent_status_at(d.applied_index(), op(400))
            .unwrap()
            .unwrap(),
    };
    let mut s = local_parent_source(&child);
    let boot = s.bootstrap_command(200000).unwrap();
    commit(&mut s, 100, boot);
    for n in 1..=32 {
        let mut h = hint(200);
        h.scope = range(0, 256);
        commit(
            &mut s,
            n,
            encode_routed(
                h,
                &[200],
                &encode_add(&[200], 1, b"effect", 1024).unwrap(),
                4096,
            )
            .unwrap(),
        );
    }
    assert_eq!(s.routed().remaining_operations(), 0);
    let bytes = command.encode(200000).unwrap();
    let original = commit(&mut s, 900, bytes.clone()).outcome;
    assert!(matches!(original, RoutedOutcome::ParentAdopted(_)));
    assert_eq!(s.grant(), &command.after());
    let next = plan.updated_manifests();
    let reverse = ReparentPlan::new(next[1].clone(), next[0].clone(), next[2].clone()).unwrap();
    commit(&mut d, 401, reverse.encode(200000).unwrap());
    let reverse = OwnerParentAdoption {
        metadata_configuration: ConfigurationId::new(1).unwrap(),
        decision: d
            .reparent_status_at(d.applied_index(), op(401))
            .unwrap()
            .unwrap(),
    };
    let cp = s.checkpoint(200000).unwrap();
    assert!(s
        .validate_proposal(
            op(901),
            &reverse.encode(200000).unwrap(),
            std::iter::empty()
        )
        .is_err());
    assert_eq!(s.checkpoint(200000).unwrap(), cp);
    let epoch = s.grant().input().epoch;
    commit(&mut s, 999, encode_fence(epoch));
    assert_eq!(commit(&mut s, 900, bytes).outcome, original);
    assert_eq!(s.routed().application().outbox().count(), 32);
}

fn prepare_parent_move(
    d: &mut Directory,
    p: &mut Directory,
    destination: &mut Directory,
    plan: &CrossReparentPlan,
) -> ConfigurationId {
    assert_eq!(
        commit(
            d,
            4000,
            PrepareReparent {
                plan: plan.clone(),
                coordinator: None
            }
            .encode(MAX_REPARENT_PREPARE_BYTES)
            .unwrap()
        )
        .outcome,
        DirectoryOutcome::ReparentGuarded
    );
    let cfg = ConfigurationId::new(1).unwrap();
    let evidence = ReparentGuardEvidence::from_status(
        cfg,
        &d.reparent_guard_at(d.applied_index(), op(4000))
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    for n in [p, destination] {
        assert_eq!(
            commit(
                n,
                4000,
                PrepareReparent {
                    plan: plan.clone(),
                    coordinator: Some(evidence)
                }
                .encode(MAX_REPARENT_PREPARE_BYTES)
                .unwrap()
            )
            .outcome,
            DirectoryOutcome::ReparentGuarded
        );
    }

    cfg
}

fn finish_parent_move(
    d: &mut Directory,
    p: &mut Directory,
    destination: &mut Directory,
    cfg: ConfigurationId,
) -> ReleaseCommittedReparent {
    let pubs = [&*d, &*p, &*destination]
        .into_iter()
        .map(|n| {
            ReparentPublicationEvidence::from_status(
                cfg,
                n.reparent_publication_at(n.applied_index(), op(4000))
                    .unwrap()
                    .unwrap(),
            )
            .unwrap()
        })
        .collect();
    assert_eq!(
        commit(
            d,
            4003,
            FinishReparent::new(op(4000), pubs)
                .unwrap()
                .encode(MAX_REPARENT_COMPLETION_BYTES)
                .unwrap()
        )
        .outcome,
        DirectoryOutcome::ReparentCompleted
    );
    let completion = ReleaseCommittedReparent {
        configuration: cfg,
        completion: d
            .reparent_completion_at(d.applied_index(), op(4000))
            .unwrap()
            .unwrap(),
    };
    for n in [p, destination] {
        assert_eq!(
            commit(n, 4004, completion.encode().unwrap()).outcome,
            DirectoryOutcome::ReparentReleased
        );
    }

    completion
}

fn reserve_moved_child(
    d: &mut Directory,
    p: &mut Directory,
    before: &ResponsibilityManifest,
) -> (
    ResponsibilityManifest,
    ResponsibilityManifest,
    TransferIntent,
) {
    let mut child = before.clone().into_input();
    child.responsibility = rid(22);
    child.parent = Some(ParentAuthority {
        responsibility: before.input().responsibility,
        group: before.input().authority,
    });
    child.scope = range(128, 192);
    child.epoch = OwnershipEpoch::new(1).unwrap();
    child.generation = RouteGeneration::new(1).unwrap();
    child.execution = ExecutionMode::Single(group(22));
    let child = ResponsibilityManifest::new(child).unwrap();
    let creation = GroupCreationIntent {
        authority: before.input().authority,
        parent: before.input().responsibility,
        expected: before.input().generation,
        responsibility: child.input().responsibility,
        bootstrap: support::bootstrap(22, 3),
        application: before.input().application,
        mode: GroupCreationMode::Staging,
    };
    assert_eq!(
        commit(d, 22, creation.encode(100000).unwrap()).outcome,
        DirectoryOutcome::CreationReserved
    );
    let created = d
        .group_creation_at(d.applied_index(), group(22))
        .unwrap()
        .unwrap();
    let mut after = before.clone().into_input();
    after.epoch = OwnershipEpoch::new(3).unwrap();
    after.generation = RouteGeneration::new(before.input().generation.get() + 1).unwrap();
    let ExecutionMode::Delegated(ref mut routes) = after.execution else {
        panic!("delegated")
    };
    routes[1].scope = range(128, 192);
    routes[1].target = RouteTarget::Child(ChildAuthority {
        responsibility: child.input().responsibility,
        group: child.input().authority,
        epoch: child.input().epoch,
    });
    routes.push(RouteEntry {
        scope: range(192, 256),
        target: RouteTarget::Group(group(20)),
    });
    let after = ResponsibilityManifest::new(after).unwrap();
    let reservation = DelegationPlan::retained_insertion(
        p.manifest(rid(600)).unwrap().clone(),
        before.clone(),
        after.clone(),
        InsertionChild::from_creation(child.clone(), &created).unwrap(),
        op(202),
    )
    .unwrap();
    assert_eq!(
        commit(p, 6000, reservation.encode(200000).unwrap()).outcome,
        DirectoryOutcome::DelegationReserved
    );
    let intent = p
        .delegation_reservation_at(p.applied_index(), op(6000))
        .unwrap()
        .unwrap()
        .child_intent(ConfigurationId::new(1).unwrap())
        .unwrap();
    assert_eq!(
        commit(d, 202, intent.encode(100000).unwrap()).outcome,
        DirectoryOutcome::TransferIntentRecorded
    );

    (child, after, intent)
}

fn activate_moved_child(
    d: &mut Directory,
    p: &mut Directory,
    s: &Source,
    intent: &TransferIntent,
    after: &ResponsibilityManifest,
    status: ScopedExportStatus,
) -> (Target, TransferPublicationStatus) {
    let mut target = Target::new(
        group(22),
        op(202),
        intent.clone(),
        BucketCounter::new(range(128, 192), Policy, bucket_limits()).unwrap(),
        Policy,
        TargetLimits {
            import_bytes: 32768,
            application_checkpoint_bytes: bucket_limits().checkpoint_bound().unwrap(),
        },
    )
    .unwrap_or_else(|e| panic!("{:?}", e.0));
    let boot = target.bootstrap_command(100000).unwrap();
    commit(&mut target, 202, boot);
    let import = TargetImport::new(
        op(202),
        intent.clone(),
        group(22),
        vec![SourceImport {
            fence: status.fence.fence,
            configuration: ConfigurationId::new(1).unwrap(),
            image: s.export(op(202), 65536).unwrap(),
            digest: status.digest,
        }],
    )
    .unwrap_or_else(|e| panic!("{:?}", e.0));
    let bytes = target.import_command(&import, 100000).unwrap();
    commit(&mut target, 202, bytes);
    let publication = TransferPublication::new(
        op(202),
        intent.clone(),
        vec![SourceFenceEvidence::from_scoped_status(
            ConfigurationId::new(1).unwrap(),
            status,
            intent,
        )
        .unwrap()],
        vec![
            TargetReadyEvidence::from_status(ConfigurationId::new(1).unwrap(), target.status())
                .unwrap(),
        ],
    )
    .unwrap_or_else(|e| panic!("{:?}", e.0));
    assert_eq!(
        commit(d, 203, publication.encode(100000).unwrap()).outcome,
        DirectoryOutcome::TransferPublished(after.input().generation)
    );
    let decision = d
        .transfer_publication_at(d.applied_index(), op(202))
        .unwrap()
        .unwrap();
    let completion = DelegationCompletion {
        reservation: op(6000),
        reservation_index: p
            .delegation_reservation_at(p.applied_index(), op(6000))
            .unwrap()
            .unwrap()
            .index,
        parent_configuration: ConfigurationId::new(1).unwrap(),
        child_configuration: ConfigurationId::new(1).unwrap(),
        decision: decision.clone(),
    };
    assert!(matches!(
        commit(p, 6001, completion.encode(200000).unwrap()).outcome,
        DirectoryOutcome::DelegationPublished(_)
    ));
    *p = recover(p);
    assert!(p
        .delegation_publication_at(p.applied_index(), op(6000))
        .unwrap()
        .is_some());
    let activation = target
        .activation_command(
            &TargetActivation {
                metadata_configuration: ConfigurationId::new(1).unwrap(),
                decision: decision.clone(),
            },
            100000,
        )
        .unwrap();
    commit(&mut target, 202, activation);

    (target, decision)
}

fn check_moved_child_retry(target: &mut Target, child: &ResponsibilityManifest) {
    let hint = RouteHint {
        responsibility: child.input().responsibility,
        group: group(22),
        application: child.input().application,
        scheme: child.input().scheme,
        scope: child.input().scope,
        bucket: 150,
        epoch: child.input().epoch,
        generation: child.input().generation,
    };
    assert_eq!(
        target
            .read_at(
                target.applied_index(),
                TargetQuery::Data(RoutedQuery {
                    hint,
                    key: vec![150],
                    query: vec![150]
                })
            )
            .unwrap(),
        TargetRead::Data(5)
    );
    let write = encode_routed(
        hint,
        &[150],
        &encode_add(&[150], 5, b"second", 1024).unwrap(),
        4096,
    )
    .unwrap();
    assert!(matches!(commit(target,6,write).outcome,TargetOutcome::Applied(r) if r.duplicate));
}

#[cfg(feature = "native")]
fn seed_moved_parent(
    entries: &[voteboat::log::LogEntry],
    boundary: u64,
    limits: voteboat::log::LogLimits,
) -> (
    support::ModelIo,
    voteboat::native::log_store::NativeLogStore<support::ModelIo>,
) {
    use support::ModelIo;
    use voteboat::{log::*, native::log_store::*};
    let io = ModelIo::default();
    let mut log = NativeLogStore::create(io.clone(), support::identity(1), limits).unwrap();
    support::append(
        &mut log,
        vec![LogMutation::Create(support::bootstrap(20, 3))],
    );
    let state = log.state(group(20)).unwrap();
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
}

fn local_parent_source(child: &ResponsibilityManifest) -> Source {
    let inner = RoutedApplication::new(
        group(20),
        child.clone(),
        BucketCounter::new(range(0, 256), Policy, bucket_limits()).unwrap(),
        Policy,
        routed().limits(),
    )
    .unwrap_or_else(|_| panic!("source"))
    .with_scoped_fencing(2)
    .unwrap_or_else(|_| panic!("scoped"));
    let s = Source::new(inner, 65536)
        .unwrap_or_else(|_| panic!("source"))
        .with_retained_insertion()
        .unwrap_or_else(|_| panic!("bound"))
        .with_retained_grants()
        .unwrap_or_else(|_| panic!("grants"))
        .with_parent_adoption(1)
        .unwrap_or_else(|_| panic!("parents"));

    s
}

fn check_remaining_parent_data(
    s: &mut Source,
    first: &TransferIntent,
    after: &ResponsibilityManifest,
) {
    let mut hint = source_hint(first, 200);
    hint.epoch = after.input().epoch;
    hint.generation = after.input().generation;
    hint.scope = range(192, 256);
    assert_eq!(
        s.read_at(
            s.applied_index(),
            ScopedSourceQuery::Data(RoutedQuery {
                hint,
                key: vec![200],
                query: vec![200]
            })
        )
        .unwrap(),
        ScopedSourceRead::Data(RoutedRead::Served(16))
    );
    let write = encode_routed(
        hint,
        &[200],
        &encode_add(&[200], 11, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap();
    assert!(matches!(commit(s,2,write).outcome,RoutedOutcome::Applied(r) if r.duplicate));
}
