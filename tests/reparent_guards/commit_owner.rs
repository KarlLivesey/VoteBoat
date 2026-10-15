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
fn selected(m: &ResponsibilityManifest, n: usize) -> Owner {
    RoutedApplication::new(
        group(21),
        m.clone(),
        BucketCounter::new(range(0, 128), Policy, bucket_limits()).unwrap(),
        Policy,
        RoutedLimits {
            operations: 32,
            semantic_bytes: 8192,
            payload_bytes: 1024,
            inner_checkpoint_bytes: bucket_limits().checkpoint_bound().unwrap(),
        },
    )
    .unwrap_or_else(|_| panic!("owner"))
    .with_cross_authority_parent_adoption(n)
    .unwrap_or_else(|_| panic!("profile"))
}
fn source(m: &ResponsibilityManifest) -> Source {
    TransferSource::new(selected(m, 2), 65536).unwrap_or_else(|_| panic!("source"))
}
fn moved() -> (Vec<Directory>, CrossOwnerParentAdoption) {
    let (p, mut nodes, c) = prepared_with(32);
    let d = decide(&mut nodes, &c);
    publish(&mut nodes, &d);
    let done = completion(&mut nodes);
    let a = CrossOwnerParentAdoption {
        plan: p,
        decision: PublishReparent {
            configuration: ConfigurationId::new(3).unwrap(),
            decision: d,
        },
        child_configuration: ConfigurationId::new(3).unwrap(),
        child_publication: nodes[2]
            .reparent_publication_at(nodes[2].applied_index(), op(200))
            .unwrap()
            .unwrap(),
        completion: ReleaseCommittedReparent {
            configuration: ConfigurationId::new(3).unwrap(),
            completion: done,
        },
    };
    for node in &mut nodes[1..] {
        assert_eq!(
            commit(node, 501, a.completion.encode().unwrap()).outcome,
            DirectoryOutcome::ReparentReleased
        );
    }
    (nodes, a)
}
fn data(m: &ResponsibilityManifest, key: u8, delta: i64) -> Vec<u8> {
    encode_routed(
        resolve(&View(vec![m.clone()]), &Policy, id(11), &[key], 1).unwrap(),
        &[key],
        &encode_add(&[key], delta, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap()
}
#[test]
fn completed_cross_move_adoption_keeps_data_retry_status_and_exact_profile_recovery() {
    let (_, a) = moved();
    let bytes = a.encode(MAX_CROSS_PARENT_ADOPTION_BYTES).unwrap();
    assert_eq!(CrossOwnerParentAdoption::decode(&bytes).unwrap(), a);
    assert!(a.encode(bytes.len() - 1).is_err());
    for end in 0..bytes.len() {
        assert!(CrossOwnerParentAdoption::decode(&bytes[..end]).is_err());
    }
    let mut owner = selected(a.before(), 1);
    let binding = owner.bootstrap_command(200000).unwrap();
    assert!(binding.starts_with(b"VBROWN04"));
    commit(&mut owner, 100, binding.clone());
    for n in 1..=32 {
        commit(&mut owner, n, data(a.before(), 1, 1));
    }
    assert_eq!(owner.remaining_operations(), 0);
    let old = owner.checkpoint(200000).unwrap();
    assert!(owner
        .apply_batch(&[
            entry(owner.applied_index() + 1, 300, bytes.clone()),
            entry(owner.applied_index() + 2, 999, vec![0])
        ])
        .is_err());
    assert_eq!(owner.checkpoint(200000).unwrap(), old);
    let status = commit(&mut owner, 300, bytes.clone()).outcome;
    assert!(matches!(status, RoutedOutcome::ParentAdopted(_)));
    assert_eq!(owner.grant(), &a.after());
    assert_eq!(owner.bootstrap_command(200000).unwrap(), binding);
    let checkpoint = owner.checkpoint(200000).unwrap();
    let mut recovered = check_truncated_owner_checkpoint(&a, &owner, &checkpoint);
    recovered
        .restore_checkpoint(4, owner.applied_index(), &checkpoint)
        .unwrap();
    recovered
        .restore_checkpoint(4, owner.applied_index(), &checkpoint)
        .unwrap();
    assert_eq!(recovered.grant(), &a.after());
    assert_eq!(commit(&mut recovered, 300, bytes.clone()).outcome, status);
    assert!(
        matches!(commit(&mut recovered,1,data(&a.after(),1,1)).outcome,RoutedOutcome::Applied(r) if r.duplicate)
    );
    assert_eq!(recovered.application().outbox().count(), 32);
    let mut changed = a.clone();
    changed.child_configuration = ConfigurationId::new(4).unwrap();
    assert_eq!(
        commit(&mut recovered, 300, changed.encode(200000).unwrap()).outcome,
        RoutedOutcome::OperationConflict
    );
    commit(&mut recovered, 900, encode_fence(a.after().input().epoch));
    assert_eq!(commit(&mut recovered, 300, bytes.clone()).outcome, status);
    let old = RoutedApplication::new(
        group(21),
        a.before().clone(),
        BucketCounter::new(range(0, 128), Policy, bucket_limits()).unwrap(),
        Policy,
        recovered.limits(),
    )
    .unwrap_or_else(|_| panic!("old"));
    let mut local = old
        .with_parent_adoption(1)
        .unwrap_or_else(|_| panic!("local"));
    let b = local.bootstrap_command(200000).unwrap();
    commit(&mut local, 100, b);
    assert!(local
        .validate_proposal(op(300), &bytes, std::iter::empty())
        .is_err());
    assert!(local
        .restore_checkpoint(4, owner.applied_index(), &checkpoint)
        .is_err());
    check_corrupt_adoption(&a);
}

#[cfg(feature = "native")]
#[test]
fn native_cache_cross_move_refresh_is_explicit_atomic_and_rejects_partial_routes() {
    use voteboat::native::routing::NativeManifestCache;
    let (_, a) = moved();
    let expected = a.plan.updated_manifests();
    let limits = ManifestCacheLimits {
        manifests: 8,
        bytes: MAX_CACHE_BYTES,
    };
    for local in [false, true] {
        let mut old = NativeManifestCache::new(limits).unwrap();
        if local {
            old = old
                .with_local_reparenting()
                .unwrap_or_else(|_| panic!("local"));
        }
        old.admit(a.before().clone()).unwrap();
        let usage = old.usage();
        assert_eq!(
            old.admit(a.after()).unwrap_err().0,
            RoutingError::IdentityChange
        );
        assert_eq!(old.usage(), usage);
    }
    for child_first in [false, true] {
        let mut cache = NativeManifestCache::new(limits)
            .unwrap()
            .with_cross_authority_reparenting()
            .unwrap_or_else(|_| panic!("cache"));
        for m in a.plan.manifests() {
            cache.admit(m.clone()).unwrap();
        }
        if child_first {
            cache.admit(expected[2].clone()).unwrap();
        } else {
            cache.admit(expected[1].clone()).unwrap();
        }
        assert!(resolve(&cache, &Policy, id(30), &[1], 3).is_err());
        if child_first {
            cache.admit(expected[1].clone()).unwrap();
        } else {
            cache.admit(expected[2].clone()).unwrap();
        }
        cache.admit(expected[0].clone()).unwrap();
        assert_eq!(
            resolve(&cache, &Policy, id(30), &[1], 3).unwrap().group,
            group(21)
        );
        assert_eq!(
            resolve(&cache, &Policy, id(10), &[1], 3),
            Err(RoutingError::Vacant)
        );
        assert_eq!(
            cache.admit(a.before().clone()).unwrap_err().0,
            RoutingError::StaleGeneration
        );
        assert!(cache.with_cross_authority_reparenting().is_err());
    }
}

fn reserve_split(nodes: &mut [Directory], a: &CrossOwnerParentAdoption) -> TransferIntent {
    let before = a.after();
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
    let plan = DelegationPlan::new(
        nodes[1].manifest(id(30)).unwrap().clone(),
        before,
        ResponsibilityManifest::new(after).unwrap(),
        op(800),
    )
    .unwrap();
    assert_eq!(
        commit(&mut nodes[1], 700, plan.encode(200000).unwrap()).outcome,
        DirectoryOutcome::DelegationReserved
    );
    let intent = nodes[1]
        .delegation_reservation_at(nodes[1].applied_index(), op(700))
        .unwrap()
        .unwrap()
        .child_intent(ConfigurationId::new(3).unwrap())
        .unwrap();
    assert_eq!(
        commit(&mut nodes[2], 800, intent.encode(200000).unwrap()).outcome,
        DirectoryOutcome::TransferIntentRecorded
    );
    intent
}
fn source_reopen(a: &Source, m: &ResponsibilityManifest) -> Source {
    let mut b = source(m);
    b.restore_checkpoint(
        a.schema_version(),
        a.applied_index(),
        &a.checkpoint(2000000).unwrap(),
    )
    .unwrap();
    b
}
#[test]
fn cross_authority_adopted_source_restarts_and_splits_under_new_parent() {
    let (mut nodes, adoption) = moved();
    let cfg = ConfigurationId::new(3).unwrap();
    let mut s = source(adoption.before());
    let boot = s.bootstrap_command(200000).unwrap();
    commit(&mut s, 100, boot);
    for (op, key, delta) in [(1, 1, 7), (2, 100, 11)] {
        assert!(matches!(
            commit(&mut s, op, data(adoption.before(), key, delta)).outcome,
            RoutedOutcome::Applied(_)
        ));
    }
    let adopted = commit(&mut s, 300, adoption.encode(200000).unwrap()).outcome;
    assert!(matches!(adopted, RoutedOutcome::ParentAdopted(_)));
    s = source_reopen(&s, adoption.before());
    assert_eq!(s.routed().grant(), &adoption.after());
    let intent = reserve_split(&mut nodes, &adoption);
    let freeze = Source::freeze_command(&intent, 200000).unwrap();
    commit(&mut s, 800, freeze);
    let SourceRead::Freeze(Some(frozen)) =
        s.read_at(s.applied_index(), SourceQuery::Freeze).unwrap()
    else {
        panic!("frozen")
    };
    s = source_reopen(&s, adoption.before());
    let (mut targets, decision) = publish_split(&mut nodes, &s, &intent, &frozen, cfg);
    let reservation_index = nodes[1]
        .delegation_reservation_at(nodes[1].applied_index(), op(700))
        .unwrap()
        .unwrap()
        .index;
    let completion = DelegationCompletion {
        reservation: op(700),
        reservation_index,
        parent_configuration: cfg,
        child_configuration: cfg,
        decision: decision.clone(),
    };
    assert!(matches!(
        commit(&mut nodes[1], 701, completion.encode(200000).unwrap()).outcome,
        DirectoryOutcome::DelegationPublished(_)
    ));
    nodes[1] = reopen13(&nodes[1], &adoption.plan);
    nodes[2] = reopen13(&nodes[2], &adoption.plan);
    let parent = nodes[1]
        .manifest(adoption.plan.new_parent().input().responsibility)
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
        commit(target, 800, command);
        let hint = resolve(
            &cache,
            &Policy,
            adoption.plan.new_parent().input().responsibility,
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
    check_adopted_frozen_source(s, &adoption, adopted, &frozen);
}
#[cfg(feature = "native")]
#[test]
fn native_cross_adoption_and_later_freeze_cuts_keep_exact_original_lineage() {
    use support::Fault;
    use voteboat::{log::*, native::log_store::*};
    let (mut nodes, adoption) = moved();
    let intent = reserve_split(&mut nodes, &adoption);
    let prototype = source(adoption.before());
    let entries = [
        entry(1, 100, prototype.bootstrap_command(200000).unwrap()),
        entry(2, 1, data(adoption.before(), 1, 7)),
        entry(3, 300, adoption.encode(200000).unwrap()),
        entry(4, 800, Source::freeze_command(&intent, 200000).unwrap()),
    ];
    let limits = LogLimits::default();
    for boundary in [3, 4] {
        let seed = || seed_adoption_journal(&entries, boundary, limits);
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
            let mut app = source(adoption.before());
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
                adoption.before().clone()
            };
            assert_eq!(app.routed().grant(), &expected);
            assert_eq!(app.fence().is_some(), state.commit_index == 4);
            assert_eq!(app.routed().application().outbox().count(), 1);
            app = source_reopen(&app, adoption.before());
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

fn check_corrupt_adoption(a: &CrossOwnerParentAdoption) {
    let mut corrupt = a.clone();
    corrupt.child_publication.index = 0;
    assert!(corrupt.encode(200000).is_err());
    let mut corrupt = a.clone();
    corrupt.completion.completion.index = corrupt.decision.decision.index;
    assert!(corrupt.encode(200000).is_err());
    let mut corrupt = a.clone();
    corrupt.decision.decision.commit.guards.pop();
    assert!(corrupt.encode(200000).is_err());
    let mut corrupt = a.clone();
    corrupt.child_publication.decision_digest = ContentDigest([0; 32]);
    assert!(corrupt.encode(200000).is_err());
}

fn publish_split(
    nodes: &mut [Directory],
    s: &Source,
    intent: &TransferIntent,
    frozen: &SourceFreezeStatus,
    cfg: ConfigurationId,
) -> (
    Vec<TransferTarget<BucketCounter<Policy>, Policy>>,
    TransferPublicationStatus,
) {
    let mut targets = Vec::new();
    let mut ready = Vec::new();
    for (g, start, end) in [(31, 0, 64), (32, 64, 128)] {
        let mut target = TransferTarget::new(
            group(g),
            op(800),
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
        commit(&mut target, 800, boot);
        let import = TargetImport::new(
            op(800),
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
        commit(&mut target, 800, command);
        ready.push(
            TargetReadyEvidence::from_status(cfg, target.status())
                .unwrap_or_else(|e| panic!("ready: {:?}", e.0)),
        );
        targets.push(target);
    }
    let publication = TransferPublication::new(
        op(800),
        intent.clone(),
        vec![SourceFenceEvidence::from_status(cfg, frozen.clone())
            .unwrap_or_else(|e| panic!("fence: {:?}", e.0))],
        ready,
    )
    .unwrap_or_else(|e| panic!("publication: {:?}", e.0));
    assert_eq!(
        commit(&mut nodes[2], 801, publication.encode(200000).unwrap()).outcome,
        DirectoryOutcome::TransferPublished(intent.after().input().generation)
    );
    let decision = nodes[2]
        .transfer_publication_at(nodes[2].applied_index(), op(800))
        .unwrap()
        .unwrap();

    (targets, decision)
}

fn check_adopted_frozen_source(
    mut s: Source,
    adoption: &CrossOwnerParentAdoption,
    adopted: RoutedOutcome<<BucketCounter<Policy> as StateMachine>::Receipt>,
    frozen: &SourceFreezeStatus,
) {
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
    s = source_reopen(&s, adoption.before());
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

#[cfg(feature = "native")]
fn seed_adoption_journal(
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
    let mut log = NativeLogStore::create(io.clone(), support::identity(21), limits).unwrap();
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
}

fn check_truncated_owner_checkpoint(
    a: &CrossOwnerParentAdoption,
    owner: &Owner,
    checkpoint: &[u8],
) -> Owner {
    let mut recovered = selected(a.before(), 1);
    let empty = recovered.checkpoint(200000).unwrap();
    for end in 0..checkpoint.len() {
        assert!(recovered
            .restore_checkpoint(4, owner.applied_index(), &checkpoint[..end])
            .is_err());
        assert_eq!(recovered.checkpoint(200000).unwrap(), empty);
    }

    recovered
}
