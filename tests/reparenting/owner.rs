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
use voteboat::{delegation::*, transfer_publication::*, transfer_source::*, transfer_target::*};
type Owner = RoutedApplication<BucketCounter<Policy>, Policy>;
type Source = TransferSource<BucketCounter<Policy>, Policy>;
fn selected(m: ResponsibilityManifest, limit: usize) -> Owner {
    owner(m, 21, range(0, 128))
        .with_parent_adoption(limit)
        .unwrap_or_else(|_| panic!("selected owner"))
}
fn source(m: ResponsibilityManifest, limit: usize) -> Source {
    TransferSource::new(selected(m, limit), 65536).unwrap_or_else(|_| panic!("source"))
}
fn metadata() -> (Directory, ReparentPlan, OwnerParentAdoption) {
    let (plan, grants) = fixture();
    let mut d = initial(grants, 32);
    assert_eq!(
        commit(&mut d, 200, plan.encode(200000).unwrap()).outcome,
        DirectoryOutcome::Reparented
    );
    let decision = d
        .reparent_status_at(d.applied_index(), op(200))
        .unwrap()
        .unwrap();
    (
        d,
        plan,
        OwnerParentAdoption {
            metadata_configuration: ConfigurationId::new(3).unwrap(),
            decision,
        },
    )
}
fn owner_reopen(a: &Owner, m: &ResponsibilityManifest, limit: usize) -> Owner {
    let mut b = selected(m.clone(), limit);
    b.restore_checkpoint(
        a.schema_version(),
        a.applied_index(),
        &a.checkpoint(1000000).unwrap(),
    )
    .unwrap();
    b
}
fn source_reopen(a: &Source, m: &ResponsibilityManifest, limit: usize) -> Source {
    let mut b = source(m.clone(), limit);
    b.restore_checkpoint(
        a.schema_version(),
        a.applied_index(),
        &a.checkpoint(1000000).unwrap(),
    )
    .unwrap();
    b
}
#[test]
fn original_owner_adopts_multiple_parent_bindings_and_keeps_original_control_retries() {
    let (mut d, plan, adoption) = metadata();
    let mut a = selected(plan.child().clone(), 4);
    let binding = a.bootstrap_command(200000).unwrap();
    assert!(binding.starts_with(b"VBROWN03"));
    commit(&mut a, 100, binding.clone());
    add(&mut a, hint_for(plan.child(), 1), 1, 1, 7);
    let bytes = adoption.encode(MAX_PARENT_ADOPTION_BYTES).unwrap();
    assert_eq!(OwnerParentAdoption::decode(&bytes).unwrap(), adoption);
    let RoutedOutcome::ParentAdopted(status) = commit(&mut a, 300, bytes.clone()).outcome else {
        panic!("adoption")
    };
    assert_eq!(status.metadata_operation, op(200));
    assert_eq!(status.metadata_index, adoption.decision.index);
    assert_eq!(a.grant(), &adoption.after());
    assert_eq!(a.bootstrap_command(200000).unwrap(), binding);
    a = owner_reopen(&a, plan.child(), 4);
    assert_eq!(a.grant(), &adoption.after());
    let next = plan.updated_manifests();
    let back = ReparentPlan::new(next[1].clone(), next[0].clone(), next[2].clone()).unwrap();
    commit(&mut d, 201, back.encode(200000).unwrap());
    let second = OwnerParentAdoption {
        metadata_configuration: ConfigurationId::new(3).unwrap(),
        decision: d
            .reparent_status_at(d.applied_index(), op(201))
            .unwrap()
            .unwrap(),
    };
    assert!(matches!(
        commit(&mut a, 301, second.encode(200000).unwrap()).outcome,
        RoutedOutcome::ParentAdopted(_)
    ));
    assert_eq!(
        commit(&mut a, 300, bytes.clone()).outcome,
        RoutedOutcome::ParentAdopted(status)
    );
    a = owner_reopen(&a, plan.child(), 4);
    assert_eq!(a.grant(), &second.after());
    add(&mut a, hint_for(&second.after(), 1), 1, 1, 7);
    assert_eq!(a.application().outbox().count(), 1);
    let epoch = a.grant().input().epoch;
    assert!(matches!(
        commit(&mut a, 900, encode_fence(epoch)).outcome,
        RoutedOutcome::Fenced(_)
    ));
    a = owner_reopen(&a, plan.child(), 4);
    assert_eq!(
        commit(&mut a, 300, bytes).outcome,
        RoutedOutcome::ParentAdopted(status)
    );
    let view = RoutedControlReads::new(a);
    let q = RoutedControlQuery::ParentAdoption(op(300));
    let r = view.read_at(view.applied_index(), q.clone()).unwrap();
    assert_eq!(r, RoutedControlRead::ParentAdoption(Some(status)));
    assert_eq!(
        view.read_result_bound(&q).unwrap(),
        std::mem::size_of::<RoutedControlRead<i64>>()
    );
    assert_eq!(view.read_result_bytes(&r, 0).unwrap(), 0);
    assert_eq!(
        view.read_at(view.applied_index() + 1, q),
        Err(ApplicationError::NotApplied)
    );
}
#[test]
fn adoption_respects_boundaries_profiles_pending_capacity_and_semantic_operations() {
    let (_, plan, adoption) = metadata();
    let bytes = adoption.encode(200000).unwrap();
    for n in 0..bytes.len() {
        assert!(OwnerParentAdoption::decode(&bytes[..n]).is_err());
    }
    assert!(adoption.encode(bytes.len() - 1).is_err());
    let mut zero = adoption.clone();
    zero.decision.index = 0;
    assert!(zero.encode(200000).is_err());
    let mut a = selected(plan.child().clone(), 1);
    let boot = a.bootstrap_command(200000).unwrap();
    commit(&mut a, 100, boot);
    assert!(a
        .validate_proposal(op(300), &bytes, [(op(300), bytes.as_slice())].into_iter())
        .is_ok());
    assert!(a
        .validate_proposal(op(301), &bytes, [(op(300), bytes.as_slice())].into_iter())
        .is_err());
    let index = a.applied_index();
    assert!(a
        .apply_batch(&[
            entry(index + 1, 300, bytes.clone()),
            entry(index + 2, 999, vec![0])
        ])
        .is_err());
    assert_eq!(a.grant(), plan.child());
    add(&mut a, hint_for(plan.child(), 1), 1, 1, 7);
    assert_eq!(
        commit(&mut a, 1, bytes.clone()).outcome,
        RoutedOutcome::OperationConflict
    );
    assert!(matches!(
        commit(&mut a, 300, bytes.clone()).outcome,
        RoutedOutcome::ParentAdopted(_)
    ));
    let mut conflict = adoption.clone();
    conflict.metadata_configuration = ConfigurationId::new(4).unwrap();
    assert_eq!(
        commit(&mut a, 300, conflict.encode(200000).unwrap()).outcome,
        RoutedOutcome::OperationConflict
    );
    assert_eq!(
        commit(&mut a, 301, bytes.clone()).outcome,
        RoutedOutcome::Rejected(RoutingError::WrongOwner)
    );
    let cp = a.checkpoint(1000000).unwrap();
    let mut old = owner(plan.child().clone(), 21, range(0, 128));
    let boot = old.bootstrap_command(200000).unwrap();
    commit(&mut old, 100, boot);
    assert!(old
        .validate_proposal(op(300), &bytes, std::iter::empty())
        .is_err());
    assert!(old.restore_checkpoint(1, a.applied_index(), &cp).is_err());
    assert!(selected(plan.child().clone(), 1)
        .with_scoped_fencing(1)
        .is_err());
    assert!(a.with_parent_adoption(1).is_err());
}

fn data(m: &ResponsibilityManifest, key: u8, delta: i64) -> Vec<u8> {
    encode_routed(
        hint_for(m, key),
        &[key],
        &encode_add(&[key], delta, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap()
}
fn reserve_split(d: &mut Directory, adoption: &OwnerParentAdoption) -> TransferIntent {
    let before = adoption.after();
    let mut after = before.clone().into_input();
    after.epoch = OwnershipEpoch::new(2).unwrap();
    after.generation = RouteGeneration::new(3).unwrap();
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
    let p = DelegationPlan::new(
        d.manifest(adoption.decision.plan.new_parent().input().responsibility)
            .unwrap()
            .clone(),
        before,
        ResponsibilityManifest::new(after).unwrap(),
        op(500),
    )
    .unwrap();
    assert_eq!(
        commit(d, 400, p.encode(200000).unwrap()).outcome,
        DirectoryOutcome::DelegationReserved
    );
    let intent = d
        .delegation_reservation_at(d.applied_index(), op(400))
        .unwrap()
        .unwrap()
        .child_intent(ConfigurationId::new(3).unwrap())
        .unwrap();
    assert_eq!(
        commit(d, 500, intent.encode(200000).unwrap()).outcome,
        DirectoryOutcome::TransferIntentRecorded
    );
    intent
}

#[test]
fn adopted_original_source_restarts_splits_and_preserves_both_target_retries() {
    let (mut d, plan, adoption) = metadata();
    let cfg = ConfigurationId::new(3).unwrap();
    let mut s = source(plan.child().clone(), 4);
    let boot = s.bootstrap_command(200000).unwrap();
    commit(&mut s, 100, boot);
    for (op, key, delta) in [(1, 1, 7), (2, 100, 11)] {
        assert!(matches!(
            commit(&mut s, op, data(plan.child(), key, delta)).outcome,
            RoutedOutcome::Applied(_)
        ));
    }
    let adopted = commit(&mut s, 300, adoption.encode(200000).unwrap()).outcome;
    assert!(matches!(adopted, RoutedOutcome::ParentAdopted(_)));
    s = source_reopen(&s, plan.child(), 4);
    assert_eq!(s.routed().grant(), &adoption.after());
    let intent = reserve_split(&mut d, &adoption);
    let freeze = Source::freeze_command(&intent, 200000).unwrap();
    commit(&mut s, 500, freeze);
    let SourceRead::Freeze(Some(frozen)) =
        s.read_at(s.applied_index(), SourceQuery::Freeze).unwrap()
    else {
        panic!("frozen")
    };
    s = source_reopen(&s, plan.child(), 4);
    let mut targets = Vec::new();
    let mut ready = Vec::new();
    for (g, start, end) in [(31, 0, 64), (32, 64, 128)] {
        let mut target = TransferTarget::new(
            group(g),
            op(500),
            intent.clone(),
            BucketCounter::new(range(start, end), Policy, bucket_limits()).unwrap(),
            Policy,
            TargetLimits {
                import_bytes: 65536,
                application_checkpoint_bytes: bucket_limits().checkpoint_bound().unwrap(),
            },
        )
        .unwrap_or_else(|e| panic!("target: {:?}", e.0));
        let boot = target.bootstrap_command(200000).unwrap();
        commit(&mut target, 500, boot);
        let import = TargetImport::new(
            op(500),
            intent.clone(),
            group(g),
            vec![SourceImport {
                fence: frozen.fence,
                configuration: cfg,
                image: s.export_target(group(g), 65536).unwrap(),
                digest: frozen
                    .exports
                    .iter()
                    .find(|e| e.target == group(g))
                    .unwrap()
                    .digest,
            }],
        )
        .unwrap_or_else(|e| panic!("import: {:?}", e.0));
        let command = target.import_command(&import, 200000).unwrap();
        commit(&mut target, 500, command);
        ready.push(
            TargetReadyEvidence::from_status(cfg, target.status())
                .unwrap_or_else(|e| panic!("ready: {:?}", e.0)),
        );
        targets.push(target);
    }
    let publication = TransferPublication::new(
        op(500),
        intent.clone(),
        vec![SourceFenceEvidence::from_status(cfg, frozen.clone())
            .unwrap_or_else(|e| panic!("fence: {:?}", e.0))],
        ready,
    )
    .unwrap_or_else(|e| panic!("publication: {:?}", e.0));
    assert_eq!(
        commit(&mut d, 501, publication.encode(200000).unwrap()).outcome,
        DirectoryOutcome::TransferPublished(intent.after().input().generation)
    );
    let decision = d
        .transfer_publication_at(d.applied_index(), op(500))
        .unwrap()
        .unwrap();
    let reservation_index = d
        .delegation_reservation_at(d.applied_index(), op(400))
        .unwrap()
        .unwrap()
        .index;
    let completion = DelegationCompletion {
        reservation: op(400),
        reservation_index,
        parent_configuration: cfg,
        child_configuration: cfg,
        decision: decision.clone(),
    };
    assert!(matches!(
        commit(&mut d, 401, completion.encode(200000).unwrap()).outcome,
        DirectoryOutcome::DelegationPublished(_)
    ));
    d = recover(&d);
    let parent = d
        .manifest(plan.new_parent().input().responsibility)
        .unwrap()
        .clone();
    let cache = View(vec![parent, intent.after().clone()]);
    for (target, (operation, key, delta)) in targets.iter_mut().zip([(1, 1, 7), (2, 100, 11)]) {
        let command = target
            .activation_command(
                &TargetActivation {
                    metadata_configuration: cfg,
                    decision: decision.clone(),
                },
                200000,
            )
            .unwrap();
        commit(target, 500, command);
        let hint = resolve(
            &cache,
            &Policy,
            plan.new_parent().input().responsibility,
            &[key],
            3,
        )
        .unwrap();
        let command = encode_routed(
            hint,
            &[key],
            &encode_add(&[key], delta, b"effect", 1024).unwrap(),
            4096,
        )
        .unwrap();
        let TargetOutcome::Applied(r) = commit(target, operation, command).outcome else {
            panic!("retry")
        };
        assert!(r.duplicate);
        assert_eq!(target.application().outbox().count(), 1);
    }
    let frozen_cp = s.checkpoint(1000000).unwrap();
    assert_eq!(
        commit(&mut s, 300, adoption.encode(200000).unwrap()).outcome,
        adopted
    );
    assert_eq!(s.fence(), Some(frozen.fence));
    let RoutedOutcome::ParentAdopted(status) = adopted else {
        unreachable!()
    };
    assert_eq!(
        s.read_at(s.applied_index(), SourceQuery::ParentAdoption(op(300)))
            .unwrap(),
        SourceRead::ParentAdoption(Some(status))
    );
    s = source_reopen(&s, plan.child(), 4);
    assert_eq!(s.fence(), Some(frozen.fence));
    assert_ne!(frozen_cp, s.checkpoint(1000000).unwrap()); // outer applied advances, frozen image does not
    for g in [31, 32] {
        assert_eq!(
            ContentDigest::scope_image(&s.export_target(group(g), 65536).unwrap()),
            frozen
                .exports
                .iter()
                .find(|e| e.target == group(g))
                .unwrap()
                .digest
        );
    }
    assert!(matches!(
        commit(&mut s, 999, data(&adoption.after(), 1, 1)).outcome,
        RoutedOutcome::Rejected(RoutingError::Fenced)
    ));
}

#[test]
fn adoption_control_reserve_and_checkpoint_chain_refuse_partial_or_conflicting_state() {
    let (mut d, plan, adoption) = metadata();
    let mut a = selected(plan.child().clone(), 1);
    let boot = a.bootstrap_command(200000).unwrap();
    commit(&mut a, 100, boot);
    for id in 1..=32 {
        add(&mut a, hint_for(plan.child(), 1), id, 1, 1);
    }
    let command = adoption.encode(200000).unwrap();
    let first = commit(&mut a, 300, command.clone()).outcome;
    assert!(matches!(first, RoutedOutcome::ParentAdopted(_)));
    let next = plan.updated_manifests();
    let back = ReparentPlan::new(next[1].clone(), next[0].clone(), next[2].clone()).unwrap();
    commit(&mut d, 201, back.encode(200000).unwrap());
    let second = OwnerParentAdoption {
        metadata_configuration: ConfigurationId::new(3).unwrap(),
        decision: d
            .reparent_status_at(d.applied_index(), op(201))
            .unwrap()
            .unwrap(),
    };
    let cp = a.checkpoint(1000000).unwrap();
    assert_eq!(
        a.apply_batch(&[entry(
            a.applied_index() + 1,
            301,
            second.encode(200000).unwrap()
        )]),
        Err(ApplicationError::ReceiptBudget)
    );
    assert_eq!(a.checkpoint(1000000).unwrap(), cp);
    let applied = a.applied_index();
    let mut fresh = selected(plan.child().clone(), 1);
    let pristine = fresh.checkpoint(1000000).unwrap();
    for end in 0..cp.len() {
        assert!(fresh.restore_checkpoint(3, applied, &cp[..end]).is_err());
        assert_eq!(fresh.checkpoint(1000000).unwrap(), pristine);
    }
    // The final adoption record follows a count, then owner operation/index/length.
    let record = cp.len() - command.len() - 28;
    for (offset, bytes) in [
        (record - 2, 2u16.to_le_bytes().to_vec()),
        (record, op(1).get().to_le_bytes().to_vec()),
        (record + 16, 2u64.to_le_bytes().to_vec()),
        (record + 28 + 32, 0u64.to_le_bytes().to_vec()),
    ] {
        let mut bad = cp.clone();
        bad[offset..offset + bytes.len()].copy_from_slice(&bytes);
        assert!(fresh.restore_checkpoint(3, applied, &bad).is_err());
        assert_eq!(fresh.checkpoint(1000000).unwrap(), pristine);
    }
    assert!(selected(plan.child().clone(), 2)
        .restore_checkpoint(3, applied, &cp)
        .is_err());
    assert!(selected(adoption.after(), 1)
        .restore_checkpoint(3, applied, &cp)
        .is_err());
    fresh.restore_checkpoint(3, applied, &cp).unwrap();
    // Restoring into an already adopted instance derives the chain from its immutable binding.
    fresh.restore_checkpoint(3, applied, &cp).unwrap();
    assert_eq!(fresh.grant(), &adoption.after());
    assert_eq!(commit(&mut fresh, 300, command).outcome, first);
    assert_eq!(fresh.application().outbox().count(), 32);
}

#[cfg(feature = "native")]
#[test]
fn native_adoption_and_subsequent_freeze_cuts_replay_only_complete_owner_states() {
    use support::{Fault, ModelIo};
    use voteboat::{log::*, native::log_store::*};
    let (mut d, plan, adoption) = metadata();
    let intent = reserve_split(&mut d, &adoption);
    let prototype = source(plan.child().clone(), 4);
    let entries = [
        entry(1, 100, prototype.bootstrap_command(200000).unwrap()),
        entry(2, 1, data(plan.child(), 1, 7)),
        entry(3, 300, adoption.encode(200000).unwrap()),
        entry(4, 500, Source::freeze_command(&intent, 200000).unwrap()),
    ];
    let limits = LogLimits::default();
    for boundary in [3, 4] {
        let seed = || {
            let io = ModelIo::default();
            let mut log =
                NativeLogStore::create(io.clone(), support::identity(21), limits).unwrap();
            support::append(
                &mut log,
                vec![LogMutation::Create(support::bootstrap(21, 3))],
            );
            let state = log.state(group(21)).unwrap();
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
            &log.state(group(21)).unwrap(),
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
            let log = NativeLogStore::recover(io, support::identity(21), limits).unwrap();
            let state = log.state(group(21)).unwrap();
            let mut app = source(plan.child().clone(), 4);
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
            let expected = if state.commit_index >= 3 {
                adoption.after()
            } else {
                plan.child().clone()
            };
            assert_eq!(app.routed().grant(), &expected);
            assert_eq!(app.fence().is_some(), state.commit_index == 4);
            assert_eq!(app.routed().application().outbox().count(), 1);
            app = source_reopen(&app, plan.child(), 4);
            let prior = app.routed().parent_adoption(op(300));
            let RoutedOutcome::ParentAdopted(status) =
                commit(&mut app, 300, adoption.encode(200000).unwrap()).outcome
            else {
                panic!("adoption retry")
            };
            assert_eq!(
                status.index,
                prior.map_or(state.commit_index + 1, |s| s.index)
            );
            if app.fence().is_some() {
                assert_eq!(
                    app.export_target(group(31), 65536)
                        .unwrap()
                        .source_applied(),
                    4
                );
            }
        }
        assert!(old && complete);
    }
}
