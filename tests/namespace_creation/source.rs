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
//! Local committed readiness/activation guard over the existing routed application.
use super::*;
use voteboat::{
    bucket_counter::*, transfer::*, transfer_publication::*, transfer_source::*, transfer_target::*,
};
type Source = CreatedNamespaceSource<BucketCounter<Policy>, Policy>;
type Owner = TransferSource<BucketCounter<Policy>, Policy>;
type NewTarget = TransferTarget<BucketCounter<Policy>, Policy>;
fn bucket_limits() -> BucketCounterLimits {
    BucketCounterLimits {
        operations: 8,
        semantic_bytes: 8192,
    }
}
fn source(p: &NamespacePlan, budget: usize) -> Source {
    let app = BucketCounter::new(p.manifest.input().scope, Policy, bucket_limits())
        .unwrap_or_else(|_| panic!("bucket app"));
    let routed = RoutedApplication::new(
        group(100),
        p.manifest.clone(),
        app,
        Policy,
        RoutedLimits {
            operations: 8,
            semantic_bytes: 8192,
            payload_bytes: 1024,
            inner_checkpoint_bytes: bucket_limits().checkpoint_bound().unwrap(),
        },
    )
    .unwrap_or_else(|_| panic!("routed"));
    let owner = Owner::new(routed, budget).unwrap_or_else(|_| panic!("source"));
    Source::from_source(p.clone(), owner).unwrap_or_else(|_| panic!("namespace source"))
}
fn reserve_source() -> (Directory, NamespacePlan) {
    reserve_directory(
        directory(16)
            .with_namespace_transfers()
            .unwrap_or_else(|_| panic!("schema4")),
    )
}
fn initialized(d: &mut Directory, p: &NamespacePlan) -> Source {
    let mut s = source(p, 65536);
    let b = s.initialization_command(100000).unwrap();
    assert_eq!(commit(&mut s, 3, b).outcome, NamespaceOutcome::Ready);
    let pubn = NamespacePublication::from_status(p, s.status()).unwrap();
    commit(d, 4, pubn.encode(1024).unwrap());
    s
}
fn activated(d: &mut Directory, p: &NamespacePlan) -> Source {
    let mut s = initialized(d, p);
    let status = d
        .namespace_publication_at(d.applied_index(), p.creation.operation)
        .unwrap()
        .unwrap();
    let b = s.activation_command(&status, 100000).unwrap();
    assert_eq!(commit(&mut s, 4, b).outcome, NamespaceOutcome::Activated);
    s
}
fn hint(m: &ResponsibilityManifest, g: u128, key: u8, scope: BucketRange) -> RouteHint {
    let m = m.input();
    RouteHint {
        scheme: m.scheme,
        scope,
        bucket: key.into(),
        responsibility: m.responsibility,
        group: group(g),
        application: m.application,
        epoch: m.epoch,
        generation: m.generation,
    }
}
fn data(m: &ResponsibilityManifest, g: u128, key: u8, scope: BucketRange, delta: i64) -> Vec<u8> {
    encode_routed(
        hint(m, g, key, scope),
        &[key],
        &encode_add(&[key], delta, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap()
}
fn query(p: &NamespacePlan, key: u8) -> SourceNamespaceQuery<Vec<u8>> {
    SourceNamespaceQuery::Data(SourceQuery::Data(RoutedQuery {
        hint: hint(&p.manifest, 100, key, p.manifest.input().scope),
        key: vec![key],
        query: vec![key],
    }))
}
fn intent(p: &NamespacePlan) -> TransferIntent {
    let mut after = p.manifest.clone().into_input();
    after.epoch = OwnershipEpoch::new(2).unwrap();
    after.generation = RouteGeneration::new(2).unwrap();
    after.execution = ExecutionMode::Partitioned(vec![
        RouteEntry {
            scope: BucketRange::new(0, 128).unwrap(),
            target: RouteTarget::Group(group(101)),
        },
        RouteEntry {
            scope: BucketRange::new(128, 256).unwrap(),
            target: RouteTarget::Group(group(102)),
        },
    ]);
    TransferIntent::new(
        p.manifest.clone(),
        ResponsibilityManifest::new(after).unwrap(),
    )
    .unwrap()
}
fn recover(s: &Source, p: &NamespacePlan) -> Source {
    let mut result = source(p, 65536);
    result
        .restore_checkpoint(2, s.applied_index(), &s.checkpoint(100000).unwrap())
        .unwrap();
    assert_eq!(result.status(), s.status());
    result
}

#[test]
fn created_source_never_serves_or_freezes_before_activation_and_rejects_bypasses() {
    let (mut d, p) = reserve_source();
    let mut s = source(&p, 65536);
    let freeze = Owner::freeze_command(&intent(&p), 100000).unwrap();
    let write = data(&p.manifest, 100, 4, p.manifest.input().scope, 7);
    for b in [&write, &freeze] {
        assert!(s
            .validate_proposal(OperationId::new(10).unwrap(), b, std::iter::empty())
            .is_err());
        assert_eq!(
            commit(&mut s, 10, b.to_vec()).outcome,
            NamespaceOutcome::NotActive
        );
        assert!(s.owner().fence().is_none());
    }
    assert_eq!(
        s.read_at(s.applied_index(), query(&p, 4)).unwrap(),
        SourceNamespaceRead::NotActive
    );
    let boot = s.initialization_command(100000).unwrap();
    commit(&mut s, 3, boot.clone());
    let publication = NamespacePublication::from_status(&p, s.status()).unwrap();
    commit(&mut d, 4, publication.encode(1024).unwrap());
    let ready = s.status();
    let mut s = recover(&s, &p);
    assert_eq!(s.status(), ready);
    assert!(s
        .validate_proposal(OperationId::new(10).unwrap(), &freeze, std::iter::empty())
        .is_err());
    assert_eq!(
        commit(&mut s, 10, freeze.clone()).outcome,
        NamespaceOutcome::NotActive
    );
    let status = d
        .namespace_publication_at(d.applied_index(), p.creation.operation)
        .unwrap()
        .unwrap();
    let activate = s.activation_command(&status, 100000).unwrap();
    commit(&mut s, 4, activate.clone());
    for b in [
        s.owner().bootstrap_command(100000).unwrap(),
        s.routed().bootstrap_command(100000).unwrap(),
        encode_fence(p.manifest.input().epoch),
    ] {
        let image = s.checkpoint(100000).unwrap();
        assert!(s
            .apply_batch(&[entry(s.applied_index() + 1, 10, b)])
            .is_err());
        assert_eq!(s.checkpoint(100000).unwrap(), image);
    }
    for op in [3, 4] {
        assert_eq!(
            commit(&mut s, op, freeze.clone()).outcome,
            NamespaceOutcome::OperationConflict
        );
        assert!(s.owner().fence().is_none());
    }
    // Provider command key must match the routed key even under the outer guard.
    let wrong_key = encode_routed(
        hint(&p.manifest, 100, 4, p.manifest.input().scope),
        &[4],
        &encode_add(&[5], 7, b"", 1024).unwrap(),
        4096,
    )
    .unwrap();
    assert!(s
        .validate_proposal(
            OperationId::new(20).unwrap(),
            &wrong_key,
            std::iter::empty()
        )
        .is_err());
    let image = s.checkpoint(100000).unwrap();
    assert!(s
        .apply_batch(&[entry(s.applied_index() + 1, 20, wrong_key)])
        .is_err());
    assert_eq!(s.checkpoint(100000).unwrap(), image);
}

#[test]
fn created_source_freeze_export_actual_targets_and_publication_preserve_lineage() {
    let (mut d, p) = reserve_source();
    let mut s = activated(&mut d, &p);
    commit(
        &mut s,
        20,
        data(&p.manifest, 100, 4, p.manifest.input().scope, 7),
    );
    commit(
        &mut s,
        21,
        data(&p.manifest, 100, 200, p.manifest.input().scope, 11),
    );
    let transfer = intent(&p);
    assert_eq!(
        commit(
            &mut d,
            10,
            transfer.encode(MAX_TRANSFER_INTENT_BYTES).unwrap()
        )
        .outcome,
        DirectoryOutcome::TransferIntentRecorded
    );
    let freeze = Owner::freeze_command(&transfer, 100000).unwrap();
    let bound = s
        .receipt_bytes_bound(&[entry(5, 10, freeze.clone())])
        .unwrap();
    let r = commit(&mut s, 10, freeze.clone());
    assert!(bound >= std::mem::size_of_val(&r));
    let fence = s.owner().fence().unwrap();
    assert_eq!(fence.index, 5);
    assert_eq!(
        r.outcome,
        NamespaceOutcome::Data(RoutedReceipt {
            index: 5,
            operation: OperationId::new(10).unwrap(),
            outcome: RoutedOutcome::Fenced(fence)
        })
    );
    let original_status = s.status();
    let mut s = recover(&s, &p);
    let status = read_created_fence(&s);
    let source_evidence =
        SourceFenceEvidence::from_status(ConfigurationId::new(1).unwrap(), status.clone())
            .unwrap_or_else(|e| panic!("{:?}", e.0));
    let (targets, ready) = import_created_targets(&s, &transfer, fence);
    let decision = publish_created_targets(&mut d, &transfer, source_evidence, ready);
    activate_created_targets(targets, &transfer, &decision);
    let image = s.owner().export_target(group(101), 65536).unwrap();
    let init = s.initialization_command(100000).unwrap();
    let activation = d
        .namespace_publication_at(d.applied_index(), p.creation.operation)
        .unwrap()
        .unwrap();
    let act = s.activation_command(&activation, 100000).unwrap();
    commit(&mut s, 3, init);
    commit(&mut s, 4, act);
    commit(&mut s, 10, freeze);
    assert_eq!(s.status(), original_status);
    assert_eq!(s.routed().applied_index(), fence.index);
    assert_eq!(s.owner().export_target(group(101), 65536).unwrap(), image);
    assert_eq!(
        s.read_at(s.applied_index(), query(&p, 4)).unwrap(),
        SourceNamespaceRead::Data(SourceRead::Data(RoutedRead::Rejected(RoutingError::Fenced)))
    );
    let r = commit(
        &mut s,
        22,
        data(&p.manifest, 100, 4, p.manifest.input().scope, 20),
    );
    assert!(matches!(
        r.outcome,
        NamespaceOutcome::Data(RoutedReceipt {
            outcome: RoutedOutcome::Rejected(RoutingError::Fenced),
            ..
        })
    ));
    assert_eq!(
        recover(&s, &p)
            .owner()
            .export_target(group(101), 65536)
            .unwrap(),
        image
    );
}

#[test]
fn source_guard_checkpoint_provenance_bounds_and_owner_selection_fail_closed() {
    let (mut d, p) = reserve_source();
    let mut s = activated(&mut d, &p);
    let write = data(&p.manifest, 100, 4, p.manifest.input().scope, 7);
    let freeze = Owner::freeze_command(&intent(&p), 100000).unwrap();
    assert!(s
        .validate_proposal(
            OperationId::new(10).unwrap(),
            &freeze,
            [(OperationId::new(20).unwrap(), write.as_slice())].into_iter()
        )
        .is_ok());
    assert!(s
        .validate_proposal(
            OperationId::new(20).unwrap(),
            &write,
            [(OperationId::new(10).unwrap(), freeze.as_slice())].into_iter()
        )
        .is_err());
    assert!(s
        .validate_proposal(
            OperationId::new(10).unwrap(),
            &freeze,
            std::iter::repeat_n(
                (OperationId::new(20).unwrap(), write.as_slice()),
                MAX_ROUTED_PENDING + 1
            )
        )
        .is_err());
    let entries = [
        entry(3, 20, write),
        entry(4, 10, freeze.clone()),
        entry(5, 10, freeze.clone()),
    ];
    let bound = s.receipt_bytes_bound(&entries).unwrap();
    let receipts = s.apply_batch(&entries).unwrap();
    assert!(bound >= receipts.capacity() * std::mem::size_of_val(&receipts[0]));
    let image = s.checkpoint(100000).unwrap();
    assert!(s.checkpoint(image.len() - 1).is_err());
    let mut fresh = source(&p, 65536);
    let before = fresh.checkpoint(100000).unwrap();
    for end in 0..image.len() {
        assert!(fresh
            .restore_checkpoint(2, s.applied_index(), &image[..end])
            .is_err());
        assert_eq!(fresh.checkpoint(100000).unwrap(), before);
    }
    let binding = u32::from_le_bytes(image[16..20].try_into().unwrap()) as usize;
    for (offset, value) in [(20 + binding, 4u64), (29 + binding, 4u64)] {
        let mut corrupt = image.clone();
        corrupt[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
        assert!(fresh
            .restore_checkpoint(2, s.applied_index(), &corrupt)
            .is_err());
        assert_eq!(fresh.checkpoint(100000).unwrap(), before);
    }
    let mut corrupt = image.clone();
    // Retained activation may not reuse the source freeze's operation ID.
    corrupt[37 + binding..53 + binding].copy_from_slice(&10u128.to_le_bytes());
    assert!(fresh
        .restore_checkpoint(2, s.applied_index(), &corrupt)
        .is_err());
    assert_eq!(fresh.checkpoint(100000).unwrap(), before);
    let mut wrong_budget = source(&p, 32768);
    assert!(wrong_budget
        .restore_checkpoint(2, s.applied_index(), &image)
        .is_err());
    fresh
        .restore_checkpoint(2, s.applied_index(), &image)
        .unwrap();
    assert_eq!(fresh.checkpoint(100000).unwrap(), image);
    assert!(Source::from_source(p.clone(), fresh.owner().clone()).is_err());

    reject_fixed_source_profile(&s, &p, &image);
    let mut tiny = source(&p, 1);
    let b = tiny.initialization_command(100000).unwrap();
    commit(&mut tiny, 3, b);
    let status = d
        .namespace_publication_at(d.applied_index(), p.creation.operation)
        .unwrap()
        .unwrap();
    let b = tiny.activation_command(&status, 100000).unwrap();
    commit(&mut tiny, 4, b);
    let before = tiny.checkpoint(100000).unwrap();
    assert!(tiny
        .validate_proposal(OperationId::new(10).unwrap(), &freeze, std::iter::empty())
        .is_err());
    assert!(tiny.apply_batch(&[entry(3, 10, freeze)]).is_err());
    assert_eq!(tiny.checkpoint(100000).unwrap(), before);
}

#[cfg(feature = "native")]
#[test]
fn every_torn_created_source_fence_frame_recovers_serving_or_fenced_exact_export() {
    use voteboat::{native::log_store::*, raft::Raft};
    let (mut d, p) = reserve_source();
    let active = activated(&mut d, &p);
    let init = active.initialization_command(100000).unwrap();
    let pubn = d
        .namespace_publication_at(d.applied_index(), p.creation.operation)
        .unwrap()
        .unwrap();
    let activate = active.activation_command(&pubn, 100000).unwrap();
    let write = data(&p.manifest, 100, 4, p.manifest.input().scope, 7);
    let freeze = Owner::freeze_command(&intent(&p), 100000).unwrap();
    let seed = || seed_created_fence_log(&p, &init, &activate, &write);
    let (_, log) = seed();
    let mutation = update(
        &log.state(group(100)).unwrap(),
        1,
        4,
        Some(Suffix {
            from: 4,
            entries: vec![entry(4, 10, freeze.clone())],
        }),
    );
    let frame = NativeLogCodec
        .encode_batch(3, std::slice::from_ref(&mutation), LogLimits::default())
        .unwrap();
    let mut old = false;
    let mut complete = false;
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
        let mut log = NativeLogStore::recover(io, identity(1), LogLimits::default()).unwrap();
        let core = Raft::recover(
            node(1),
            log.binding(),
            log.state(group(100)).unwrap(),
            log.limits(),
        )
        .unwrap();
        let mut app = source(&p, 65536);
        app.apply_batch(core.replay_committed()).unwrap();
        assert_eq!(app.status().ready_index, Some(1));
        assert_eq!(app.status().activation_index, Some(2));
        match core.state().commit_index {
            3 => {
                old = true;
                assert!(app.owner().fence().is_none());
                assert_eq!(
                    app.read_at(3, query(&p, 4)).unwrap(),
                    SourceNamespaceRead::Data(SourceRead::Data(RoutedRead::Served(7)))
                );
            }
            4 => {
                complete = true;
                assert_eq!(app.owner().fence().unwrap().index, 4);
                assert_eq!(
                    app.read_at(4, query(&p, 4)).unwrap(),
                    SourceNamespaceRead::Data(SourceRead::Data(RoutedRead::Rejected(
                        RoutingError::Fenced
                    )))
                );
            }
            _ => panic!("old or complete fence only"),
        }
        retry_created_fence(&mut log, &p, &freeze);
    }
    assert!(old && complete);
}

type ImportedTarget = (NewTarget, u128, u8, i64, BucketRange);
fn import_created_targets(
    s: &Source,
    transfer: &TransferIntent,
    fence: OwnershipFence,
) -> (Vec<ImportedTarget>, Vec<TargetReadyEvidence>) {
    let mut targets = Vec::new();
    let mut ready = Vec::new();
    for (g, key, expected, range) in [
        (101, 4, 7, BucketRange::new(0, 128).unwrap()),
        (102, 200, 11, BucketRange::new(128, 256).unwrap()),
    ] {
        let image = s.owner().export_target(group(g), 65536).unwrap();
        assert_eq!(image.source_applied(), fence.index);
        let mut target = NewTarget::new(
            group(g),
            OperationId::new(10).unwrap(),
            transfer.clone(),
            BucketCounter::new(range, Policy, bucket_limits()).unwrap_or_else(|_| panic!("app")),
            Policy,
            TargetLimits {
                import_bytes: 65536,
                application_checkpoint_bytes: bucket_limits().checkpoint_bound().unwrap(),
            },
        )
        .unwrap_or_else(|e| panic!("{:?}", e.0));
        let bootstrap = target.bootstrap_command(100000).unwrap();
        commit(&mut target, 10, bootstrap);
        let import = TargetImport::new(
            OperationId::new(10).unwrap(),
            transfer.clone(),
            group(g),
            vec![SourceImport {
                fence,
                configuration: ConfigurationId::new(1).unwrap(),
                digest: ContentDigest::scope_image(&image),
                image,
            }],
        )
        .unwrap_or_else(|e| panic!("{:?}", e.0));
        let b = target.import_command(&import, 100000).unwrap();
        commit(&mut target, 10, b);
        assert!(target.status().activated.is_none());
        assert_eq!(
            target
                .read_at(
                    target.applied_index(),
                    TargetQuery::Data(RoutedQuery {
                        hint: hint(transfer.after(), g, key, range),
                        key: vec![key],
                        query: vec![key],
                    })
                )
                .unwrap(),
            TargetRead::NotActive
        );
        ready.push(
            TargetReadyEvidence::from_status(ConfigurationId::new(1).unwrap(), target.status())
                .unwrap_or_else(|e| panic!("{:?}", e.0)),
        );
        targets.push((target, g, key, expected, range));
    }
    (targets, ready)
}

fn activate_created_targets(
    targets: Vec<ImportedTarget>,
    transfer: &TransferIntent,
    decision: &TransferPublicationStatus,
) {
    for (mut target, g, key, expected, range) in targets {
        let activation = TargetActivation {
            metadata_configuration: ConfigurationId::new(1).unwrap(),
            decision: decision.clone(),
        };
        let b = target.activation_command(&activation, 100000).unwrap();
        commit(&mut target, 10, b);
        let op = if key == 4 { 20 } else { 21 };
        let r = commit(
            &mut target,
            op,
            data(transfer.after(), g, key, range, expected),
        );
        assert!(
            matches!(r.outcome,TargetOutcome::Applied(BucketReceipt {duplicate:true,outcome:BucketOutcome::Value(v),..}) if v==expected)
        );
        assert_eq!(target.application().outbox().count(), 1);
    }
}

fn reject_fixed_source_profile(s: &Source, p: &NamespacePlan, image: &[u8]) {
    let mut fixed = CreatedNamespace::new(
        p.clone(),
        BucketCounter::new(p.manifest.input().scope, Policy, bucket_limits())
            .unwrap_or_else(|_| panic!("app")),
        Policy,
        s.routed().limits(),
    )
    .unwrap_or_else(|_| panic!("fixed"));
    assert_eq!(fixed.schema_version(), 1);
    assert_ne!(
        fixed.initialization_command(100000).unwrap(),
        source(p, 65536).initialization_command(100000).unwrap()
    );
    assert_eq!(
        fixed.restore_checkpoint(2, s.applied_index(), image),
        Err(ApplicationError::UnsupportedSchema)
    );
    assert!(fixed
        .restore_checkpoint(1, s.applied_index(), image)
        .is_err());
    let fixed_boot = fixed.initialization_command(100000).unwrap();
    commit(&mut fixed, 3, fixed_boot);
    let fixed_image = fixed.checkpoint(100000).unwrap();
    let mut source_fresh = source(p, 65536);
    assert_eq!(
        source_fresh.restore_checkpoint(1, fixed.applied_index(), &fixed_image),
        Err(ApplicationError::UnsupportedSchema)
    );
    assert!(source_fresh
        .restore_checkpoint(2, fixed.applied_index(), &fixed_image)
        .is_err());
}

#[cfg(feature = "native")]
fn seed_created_fence_log(
    p: &NamespacePlan,
    init: &[u8],
    activate: &[u8],
    write: &[u8],
) -> (
    ModelIo,
    voteboat::native::log_store::NativeLogStore<ModelIo>,
) {
    use voteboat::native::log_store::*;
    let io = ModelIo::default();
    let mut log = NativeLogStore::create(io.clone(), identity(1), LogLimits::default()).unwrap();
    append(
        &mut log,
        vec![LogMutation::Create(p.creation.intent.bootstrap.clone())],
    );
    let state = log.state(group(100)).unwrap();
    append(
        &mut log,
        vec![update(
            &state,
            1,
            3,
            Some(Suffix {
                from: 1,
                entries: vec![
                    entry(1, 3, init.to_vec()),
                    entry(2, 4, activate.to_vec()),
                    entry(3, 20, write.to_vec()),
                ],
            }),
        )],
    );
    (io, log)
}

#[cfg(feature = "native")]
fn retry_created_fence(
    log: &mut voteboat::native::log_store::NativeLogStore<ModelIo>,
    p: &NamespacePlan,
    freeze: &[u8],
) {
    use voteboat::raft::Raft;
    let state = log.state(group(100)).unwrap();
    let index = state.last_index() + 1;
    append(
        log,
        vec![update(
            &state,
            1,
            index,
            Some(Suffix {
                from: index,
                entries: vec![entry(index, 10, freeze.to_vec())],
            }),
        )],
    );
    let core = Raft::recover(
        node(1),
        log.binding(),
        log.state(group(100)).unwrap(),
        log.limits(),
    )
    .unwrap();
    let mut retried = source(p, 65536);
    retried.apply_batch(core.replay_committed()).unwrap();
    let fence = retried.owner().fence().unwrap();
    assert_eq!(fence.index, 4);
    let image = retried.owner().export_target(group(101), 65536).unwrap();
    assert_eq!(image.source_applied(), 4);
    assert_eq!(
        recover(&retried, p)
            .owner()
            .export_target(group(101), 65536)
            .unwrap(),
        image
    );
    assert_eq!(
        retried.read_at(index, query(p, 4)).unwrap(),
        SourceNamespaceRead::Data(SourceRead::Data(RoutedRead::Rejected(RoutingError::Fenced)))
    );
}

fn read_created_fence(s: &Source) -> SourceFreezeStatus {
    let q = SourceNamespaceQuery::Data(SourceQuery::Freeze);
    let result = s.read_at(s.applied_index(), q.clone()).unwrap();
    let nested = s.read_result_bytes(&result, usize::MAX).unwrap();
    assert_eq!(
        s.read_result_bound(&q).unwrap(),
        std::mem::size_of_val(&result) + nested
    );
    assert!(s.read_result_bytes(&result, nested - 1).is_err());
    let SourceNamespaceRead::Data(SourceRead::Freeze(Some(status))) = result else {
        panic!("frozen status")
    };
    status
}

fn publish_created_targets(
    d: &mut Directory,
    transfer: &TransferIntent,
    source_evidence: SourceFenceEvidence,
    ready: Vec<TargetReadyEvidence>,
) -> TransferPublicationStatus {
    let publication = TransferPublication::new(
        OperationId::new(10).unwrap(),
        transfer.clone(),
        vec![source_evidence],
        ready,
    )
    .unwrap_or_else(|e| panic!("{:?}", e.0));
    assert_eq!(
        commit(
            d,
            11,
            publication.encode(MAX_TRANSFER_PUBLICATION_BYTES).unwrap()
        )
        .outcome,
        DirectoryOutcome::TransferPublished(RouteGeneration::new(2).unwrap())
    );
    d.transfer_publication_at(d.applied_index(), OperationId::new(10).unwrap())
        .unwrap()
        .unwrap()
}
