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
use voteboat::{transfer::*, transfer_publication::*, transfer_source::*, transfer_target::*};

fn transfer_directory(ops: usize) -> Directory {
    directory(ops)
        .with_namespace_transfers()
        .unwrap_or_else(|_| panic!("fresh schema4"))
}
fn reserve_transfer(ops: usize) -> (Directory, NamespacePlan) {
    reserve_directory(transfer_directory(ops))
}

fn split(before: &ResponsibilityManifest) -> TransferIntent {
    let mut after = before.clone().into_input();
    after.epoch = OwnershipEpoch::new(after.epoch.get() + 1).unwrap();
    after.generation = RouteGeneration::new(after.generation.get() + 1).unwrap();
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
    TransferIntent::new(before.clone(), ResponsibilityManifest::new(after).unwrap()).unwrap()
}
fn published(ops: usize) -> (Directory, NamespacePlan) {
    let (mut d, p) = reserve_transfer(ops);
    let publication = ready(&mut target(&p));
    assert!(matches!(
        commit(&mut d, 4, publication.encode(1024).unwrap()).outcome,
        DirectoryOutcome::NamespacePublished(_)
    ));
    (d, p)
}
fn restore(d: &Directory, ops: usize) -> Directory {
    let mut fresh = transfer_directory(ops);
    fresh
        .restore_checkpoint(4, d.applied_index(), &d.checkpoint(1_000_000).unwrap())
        .unwrap();
    fresh
}

// Deliberately constructed metadata facts: these tests check the directory's
// state machine, not actual source fencing, target import or authentication.
fn evidence(intent: &TransferIntent, operation: OperationId) -> TransferPublication {
    let configuration = ConfigurationId::new(1).unwrap();
    let digest = ContentDigest::sha256(&intent.encode(MAX_TRANSFER_INTENT_BYTES).unwrap());
    let sources: Vec<_> = intent
        .sources()
        .iter()
        .map(|source| {
            let RouteTarget::Group(group) = source.target else {
                unreachable!()
            };
            SourceFenceEvidence {
                fence: OwnershipFence {
                    operation,
                    group,
                    responsibility: intent.before().input().responsibility,
                    epoch: intent.before().input().epoch,
                    index: 8,
                },
                configuration,
                intent_digest: digest,
                exports: intent
                    .targets()
                    .iter()
                    .filter_map(|target| {
                        let start = source.scope.start().max(target.scope.start());
                        let end = source.scope.end().min(target.scope.end());
                        let RouteTarget::Group(target) = target.target else {
                            unreachable!()
                        };
                        (start < end).then(|| SourceExportCommitment {
                            target,
                            scope: BucketRange::new(start, end).unwrap(),
                            digest: ContentDigest::sha256(&[start as u8, end as u8]),
                        })
                    })
                    .collect(),
            }
        })
        .collect();
    let targets = intent
        .targets()
        .iter()
        .map(|target| {
            let RouteTarget::Group(group) = target.target else {
                unreachable!()
            };
            TargetReadyEvidence {
                group,
                operation,
                configuration,
                staged_index: 1,
                imported: ImportStatus {
                    index: 2,
                    digest: ContentDigest::sha256(b"metadata fixture import"),
                    sources: sources
                        .iter()
                        .filter_map(|s| {
                            s.exports
                                .iter()
                                .find(|e| e.target == group)
                                .map(|e| ImportedSource {
                                    fence: s.fence,
                                    configuration: s.configuration,
                                    scope: e.scope,
                                    digest: e.digest,
                                })
                        })
                        .collect(),
                },
            }
        })
        .collect();
    TransferPublication::new(operation, intent.clone(), sources, targets)
        .unwrap_or_else(|e| panic!("publication shape: {:?}", e.0))
}

#[test]
fn created_namespace_requires_publication_and_exact_current_authority() {
    let (mut unpublished, p) = reserve_transfer(16);
    let intent = split(&p.manifest);
    let bytes = intent.encode(MAX_TRANSFER_INTENT_BYTES).unwrap();
    assert_eq!(
        commit(&mut unpublished, 10, bytes.clone()).outcome,
        DirectoryOutcome::UnknownResponsibility
    );
    assert_eq!(
        unpublished.reserved_publication_bytes(),
        MAX_NAMESPACE_PUBLICATION_BYTES
    );
    let publication = ready(&mut target(&p));
    commit(&mut unpublished, 4, publication.encode(1024).unwrap());
    // A failed original attempt remains failed even after publication.
    let retry = commit(&mut unpublished, 10, bytes.clone());
    assert!(retry.duplicate);
    assert_eq!(retry.outcome, DirectoryOutcome::UnknownResponsibility);
    assert_eq!(
        commit(&mut unpublished, 11, bytes).outcome,
        DirectoryOutcome::TransferIntentRecorded
    );
    assert_eq!(unpublished.manifest(identity_of(50)), Some(&p.manifest));

    let (d, p) = published(16);
    for case in 0..4 {
        let mut before = p.manifest.clone().into_input();
        match case {
            0 => before.responsibility = identity_of(51),
            1 => before.authority = group(9),
            2 => before.generation = RouteGeneration::new(2).unwrap(),
            _ => before.responsibility.incarnation = ResponsibilityIncarnation::new(2).unwrap(),
        }
        let intent = split(&ResponsibilityManifest::new(before).unwrap());
        let mut trial = d.clone();
        assert_eq!(
            commit(
                &mut trial,
                10,
                intent.encode(MAX_TRANSFER_INTENT_BYTES).unwrap()
            )
            .outcome,
            if case == 2 {
                DirectoryOutcome::GenerationMismatch
            } else {
                DirectoryOutcome::UnknownResponsibility
            }
        );
        assert!(trial
            .transfer_intent_at(trial.applied_index(), OperationId::new(10).unwrap())
            .unwrap()
            .is_none());
        assert_eq!(trial.manifest(identity_of(50)), Some(&p.manifest));
        assert_eq!(trial.reserved_publication_bytes(), 0);
    }
}

#[test]
fn created_namespace_transfer_lock_publication_and_repeated_movement_recover() {
    // Initialization, anchor publication, creation and transfer intent exhaust
    // ordinary capacity; namespace/transfer publications have reserved control.
    let (mut d, p) = published(4);
    let intent = split(&p.manifest);
    let bytes = intent.encode(MAX_TRANSFER_INTENT_BYTES).unwrap();
    assert_eq!(
        commit(&mut d, 10, bytes.clone()).outcome,
        DirectoryOutcome::TransferIntentRecorded
    );
    assert_eq!(d.remaining_operations(), 0);
    assert_eq!(
        d.reserved_publication_bytes(),
        MAX_TRANSFER_PUBLICATION_BYTES
    );
    let original = d
        .transfer_intent_at(d.applied_index(), OperationId::new(10).unwrap())
        .unwrap()
        .unwrap();
    let mut d = restore(&d, 4);
    assert_eq!(
        d.transfer_intent_at(d.applied_index(), original.operation)
            .unwrap(),
        Some(original.clone())
    );
    assert!(commit(&mut d, 10, bytes).duplicate);
    let publication = evidence(&intent, original.operation);
    let bytes = publication.encode(MAX_TRANSFER_PUBLICATION_BYTES).unwrap();
    assert!(d
        .validate_proposal(OperationId::new(11).unwrap(), &bytes, std::iter::empty())
        .is_ok());
    assert_eq!(
        commit(&mut d, 11, bytes.clone()).outcome,
        DirectoryOutcome::TransferPublished(RouteGeneration::new(2).unwrap())
    );
    assert_eq!(d.manifest(identity_of(50)), Some(intent.after()));
    assert_eq!(d.reserved_publication_bytes(), 0);
    let decision = d
        .transfer_publication_at(d.applied_index(), original.operation)
        .unwrap()
        .unwrap();
    let mut d = restore(&d, 4);
    assert!(commit(&mut d, 11, bytes).duplicate);
    assert_eq!(
        d.transfer_publication_at(d.applied_index(), original.operation)
            .unwrap(),
        Some(decision)
    );
    assert_eq!(
        d.namespace_publication_at(d.applied_index(), p.creation.operation)
            .unwrap()
            .unwrap()
            .publication
            .manifest,
        p.manifest
    );

    // A separate adequately sized history checks the lock and subsequent merge.
    let (mut d, p) = published(16);
    let intent = split(&p.manifest);
    let bytes = intent.encode(MAX_TRANSFER_INTENT_BYTES).unwrap();
    commit(&mut d, 10, bytes.clone());
    let mut d = restore(&d, 16);
    assert_eq!(
        commit(&mut d, 12, bytes).outcome,
        DirectoryOutcome::LifecycleBusy
    );
    assert_eq!(
        commit(
            &mut d,
            13,
            DirectoryCommand {
                expected: Some(RouteGeneration::new(1).unwrap()),
                manifest: intent.after().clone(),
            }
            .encode(MAX_DIRECTORY_COMMAND_BYTES)
            .unwrap()
        )
        .outcome,
        DirectoryOutcome::LifecycleBusy
    );
    commit(
        &mut d,
        11,
        evidence(&intent, OperationId::new(10).unwrap())
            .encode(MAX_TRANSFER_PUBLICATION_BYTES)
            .unwrap(),
    );
    let mut d = restore(&d, 16);
    let mut after = intent.after().clone().into_input();
    after.epoch = OwnershipEpoch::new(3).unwrap();
    after.generation = RouteGeneration::new(3).unwrap();
    after.execution = ExecutionMode::Single(group(103));
    let merge = TransferIntent::new(
        intent.after().clone(),
        ResponsibilityManifest::new(after).unwrap(),
    )
    .unwrap();
    assert_eq!(
        commit(&mut d, 20, merge.encode(MAX_TRANSFER_INTENT_BYTES).unwrap()).outcome,
        DirectoryOutcome::TransferIntentRecorded
    );
    let mut d = restore(&d, 16);
    assert_eq!(
        commit(
            &mut d,
            21,
            evidence(&merge, OperationId::new(20).unwrap())
                .encode(MAX_TRANSFER_PUBLICATION_BYTES)
                .unwrap()
        )
        .outcome,
        DirectoryOutcome::TransferPublished(RouteGeneration::new(3).unwrap())
    );
    let d = restore(&d, 16);
    assert_eq!(d.manifest(identity_of(50)), Some(merge.after()));
    assert_eq!(d.manifest(identity_of(1)), Some(&manifest(1, 20)));
}

#[test]
fn schema4_selection_preserves_schema3_refusals_and_rejects_cross_schema_history() {
    let (mut legacy, p) = reserve(16);
    let publication = ready(&mut target(&p));
    commit(&mut legacy, 4, publication.encode(1024).unwrap());
    let bytes = split(&p.manifest)
        .encode(MAX_TRANSFER_INTENT_BYTES)
        .unwrap();
    assert_eq!(
        commit(&mut legacy, 10, bytes.clone()).outcome,
        DirectoryOutcome::UnknownResponsibility
    );
    assert!(legacy.clone().with_namespace_transfers().is_err());
    let image = legacy.checkpoint(1_000_000).unwrap();
    let mut recovered = directory(16);
    recovered
        .restore_checkpoint(3, legacy.applied_index(), &image)
        .unwrap();
    assert_eq!(recovered.checkpoint(1_000_000).unwrap(), image);
    assert_eq!(
        commit(&mut recovered, 10, bytes).outcome,
        DirectoryOutcome::UnknownResponsibility
    );
    assert_eq!(recovered.reserved_publication_bytes(), 0);

    let (mut current, _) = published(16);
    commit(
        &mut current,
        10,
        split(&p.manifest)
            .encode(MAX_TRANSFER_INTENT_BYTES)
            .unwrap(),
    );
    assert_eq!(
        current.schema_version(),
        NAMESPACE_TRANSFER_DIRECTORY_APPLICATION_SCHEMA
    );
    let current_image = current.checkpoint(1_000_000).unwrap();
    for (mut destination, source) in [(directory(16), current), (transfer_directory(16), legacy)] {
        let before = destination.checkpoint(1_000_000).unwrap();
        let source_image = source.checkpoint(1_000_000).unwrap();
        assert_eq!(
            destination.restore_checkpoint(
                source.schema_version(),
                source.applied_index(),
                &source_image
            ),
            Err(ApplicationError::UnsupportedSchema)
        );
        assert!(destination
            .restore_checkpoint(
                destination.schema_version(),
                source.applied_index(),
                &source_image
            )
            .is_err());
        assert!(destination
            .apply_batch(&[entry(
                1,
                1,
                source
                    .bootstrap_command(MAX_DIRECTORY_COMMAND_BYTES)
                    .unwrap()
            )])
            .is_err());
        assert_eq!(destination.checkpoint(1_000_000).unwrap(), before);
    }
    let mut destination = transfer_directory(16);
    let before = destination.checkpoint(1_000_000).unwrap();
    // The dynamic publication and lock are reconstructed only on complete input.
    let applied = 5;
    for end in 0..current_image.len() {
        assert!(destination
            .restore_checkpoint(4, applied, &current_image[..end])
            .is_err());
        assert_eq!(destination.checkpoint(1_000_000).unwrap(), before);
    }
    destination
        .restore_checkpoint(4, applied, &current_image)
        .unwrap();
    assert_eq!(
        destination.reserved_publication_bytes(),
        MAX_TRANSFER_PUBLICATION_BYTES
    );
}
