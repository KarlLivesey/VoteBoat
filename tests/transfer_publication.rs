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
use voteboat::{
    application::*, directory::*, identity::*, routing::*, transfer::*, transfer_publication::*,
};
#[path = "transfer_source/fixtures.rs"]
pub mod source_fixture;
#[path = "transfer_target/fixtures.rs"]
mod target_fixture;
use source_fixture::{entry, group, noop, op};
fn fresh(operations: usize, history_bytes: usize) -> Directory {
    Directory::new(
        DirectoryPlan::new(group(1), vec![source_fixture::grant()]).unwrap(),
        DirectoryLimits {
            operations,
            history_bytes,
        },
    )
    .unwrap()
}
fn grant_command() -> Vec<u8> {
    DirectoryCommand {
        expected: None,
        manifest: source_fixture::grant(),
    }
    .encode(32768)
    .unwrap()
}
fn ready(operations: usize, history_bytes: usize) -> Directory {
    let mut d = fresh(operations, history_bytes);
    d.apply_batch(&[
        entry(1, 1000, d.bootstrap_command(65536).unwrap()),
        entry(2, 1001, grant_command()),
        entry(3, 200, source_fixture::intent().encode(32768).unwrap()),
    ])
    .unwrap();
    d
}
#[test]
fn complete_evidence_publishes_exact_after_manifest_and_retains_original_decision() {
    let mut d = ready(20, 65536);
    let publication = target_fixture::publication();
    let bytes = publication.encode(MAX_TRANSFER_PUBLICATION_BYTES).unwrap();
    assert_eq!(
        d.reserved_publication_bytes(),
        MAX_TRANSFER_PUBLICATION_BYTES
    );
    let receipt = d
        .apply_batch(&[entry(4, 201, bytes.clone())])
        .unwrap()
        .remove(0);
    assert_eq!(
        receipt.outcome,
        DirectoryOutcome::TransferPublished(publication.intent().after().input().generation)
    );
    assert_eq!(
        d.manifest(source_fixture::grant().input().responsibility),
        Some(publication.intent().after())
    );
    assert_eq!(d.reserved_publication_bytes(), 0);
    let status = d.transfer_publication_at(4, op(200)).unwrap().unwrap();
    assert_eq!(status.publication_operation, op(201));
    assert_eq!(status.index, 4);
    assert_eq!(status.publication, publication);
    assert!(d.apply_batch(&[entry(5, 201, bytes.clone())]).unwrap()[0].duplicate);
    assert_eq!(
        d.apply_batch(&[entry(6, 202, bytes)]).unwrap()[0].outcome,
        DirectoryOutcome::TransferEvidenceMismatch
    );
    assert_eq!(d.transfer_publication_at(6, op(200)).unwrap(), Some(status));
    // Metadata-only updates use the newly published owner as their immutable base.
    let mut next = publication.intent().after().clone().into_input();
    next.generation = RouteGeneration::new(3).unwrap();
    let next = ResponsibilityManifest::new(next).unwrap();
    let bytes = DirectoryCommand {
        expected: Some(RouteGeneration::new(2).unwrap()),
        manifest: next.clone(),
    }
    .encode(32768)
    .unwrap();
    assert_eq!(
        d.apply_batch(&[entry(7, 203, bytes)]).unwrap()[0].outcome,
        DirectoryOutcome::Published(RouteGeneration::new(3).unwrap())
    );
    assert_eq!(d.manifest(next.input().responsibility), Some(&next));
}
#[test]
fn accepted_intent_reserves_publication_after_ordinary_operation_and_byte_exhaustion() {
    let body = source_fixture::intent().encode(32768).unwrap();
    let configured = fresh(3, 65536);
    let history_bytes =
        configured.bootstrap_command(65536).unwrap().len() + grant_command().len() + body.len();
    let mut d = ready(3, history_bytes);
    assert_eq!(d.remaining_operations(), 0);
    let bytes = target_fixture::publication()
        .encode(MAX_TRANSFER_PUBLICATION_BYTES)
        .unwrap();
    assert!(d
        .validate_proposal(op(201), &bytes, std::iter::empty())
        .is_ok());
    assert!(d
        .validate_proposal(op(204), &grant_command(), std::iter::empty())
        .is_err());
    // Two different successful-decision candidates need room for a losing outcome.
    assert!(d
        .validate_proposal(op(202), &bytes, [(op(201), bytes.as_slice())].into_iter())
        .is_err());
    assert!(d
        .validate_proposal(op(201), &bytes, [(op(201), bytes.as_slice())].into_iter())
        .is_ok());
    d.apply_batch(&[entry(4, 201, bytes)]).unwrap();
    assert_eq!(d.remaining_operations(), 0);
    let checkpoint = d.checkpoint(100000).unwrap();
    let mut recovered = fresh(3, history_bytes);
    recovered.restore_checkpoint(1, 4, &checkpoint).unwrap();
    assert_eq!(recovered.checkpoint(100000).unwrap(), checkpoint);
    assert_eq!(recovered.reserved_publication_bytes(), 0);
    assert_eq!(recovered.remaining_operations(), 0);
}
#[test]
fn missing_changed_reordered_and_inconsistent_facts_refuse_before_publication() {
    let publication = target_fixture::publication();
    for case in 0..9 {
        let mut sources = publication.sources().to_vec();
        let mut targets = publication.targets().to_vec();
        match case {
            0 => {
                targets.pop();
            }
            1 => targets.reverse(),
            2 => sources[0].fence.index += 1,
            3 => sources[0].intent_digest.0[0] ^= 1,
            4 => sources[0].exports[0].digest.0[0] ^= 1,
            5 => targets[0].imported.sources[0].configuration = ConfigurationId::new(2).unwrap(),
            6 => targets[0].imported.index = targets[0].staged_index,
            7 => targets[0].group = group(99),
            _ => {
                sources[0].exports.pop();
            }
        }
        let ptr = targets.as_ptr();
        let (_, _, _, returned) =
            TransferPublication::new(op(200), source_fixture::intent(), sources, targets)
                .unwrap_err();
        assert_eq!(returned.as_ptr(), ptr);
    }
    let mut d = fresh(20, 65536);
    let original = d.checkpoint(100000).unwrap();
    let bytes = publication.encode(MAX_TRANSFER_PUBLICATION_BYTES).unwrap();
    assert!(d.apply_batch(&[entry(1, 201, bytes)]).is_err());
    assert_eq!(d.checkpoint(100000).unwrap(), original);
}
#[test]
fn codec_and_checkpoint_all_truncations_are_atomic_and_status_preserves_indices() {
    let publication = target_fixture::publication();
    let bytes = publication.encode(MAX_TRANSFER_PUBLICATION_BYTES).unwrap();
    assert_eq!(TransferPublication::decode(&bytes).unwrap(), publication);
    for end in 0..bytes.len() {
        assert!(TransferPublication::decode(&bytes[..end]).is_err());
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(TransferPublication::decode(&trailing).is_err());
    assert!(publication.encode(bytes.len() - 1).is_err());
    let mut d = ready(20, 65536);
    d.apply_batch(&[entry(4, 201, bytes), noop(5)]).unwrap();
    let checkpoint = d.checkpoint(100000).unwrap();
    for end in 0..checkpoint.len() {
        let mut r = fresh(20, 65536);
        let before = r.checkpoint(100000).unwrap();
        assert!(r.restore_checkpoint(1, 5, &checkpoint[..end]).is_err());
        assert_eq!(r.checkpoint(100000).unwrap(), before);
    }
    let mut r = fresh(20, 65536);
    r.restore_checkpoint(1, 5, &checkpoint).unwrap();
    assert_eq!(r.checkpoint(100000).unwrap(), checkpoint);
    let status = r.transfer_publication_at(5, op(200)).unwrap().unwrap();
    let encoded = status.encode(36 + MAX_TRANSFER_PUBLICATION_BYTES).unwrap();
    assert_eq!(TransferPublicationStatus::decode(&encoded).unwrap(), status);
    for end in 0..encoded.len() {
        assert!(TransferPublicationStatus::decode(&encoded[..end]).is_err());
    }
    let mut invalid = status;
    invalid.publication_operation = op(200);
    assert!(invalid.encode(100000).is_err());
}
#[test]
fn lifecycle_read_view_checks_prefix_and_accounts_all_nested_evidence() {
    let mut d = ready(20, 65536);
    assert!(d.transfer_publication_at(4, op(200)).is_err());
    d.apply_batch(&[entry(
        4,
        201,
        target_fixture::publication()
            .encode(MAX_TRANSFER_PUBLICATION_BYTES)
            .unwrap(),
    )])
    .unwrap();
    let view = LifecycleDirectory::new(d);
    let query = DirectoryQuery::Publication(op(200));
    let result = view.read_at(4, query).unwrap();
    let nested = view.read_result_bytes(&result, 100000).unwrap();
    assert_eq!(
        view.read_result_bound(&query).unwrap(),
        std::mem::size_of_val(&result) + nested
    );
    assert!(view.read_result_bytes(&result, nested - 1).is_err());
    assert_eq!(
        view.read_at(4, DirectoryQuery::Publication(op(999)))
            .unwrap(),
        DirectoryRead::Publication(None)
    );
}

#[test]
fn maximum_256_route_publication_fits_native_command_envelope() {
    use voteboat::{
        routed::OwnershipFence,
        transfer_source::SourceExportCommitment,
        transfer_target::{ImportStatus, ImportedSource},
    };
    let before = source_fixture::grant();
    let mut after = before.clone().into_input();
    after.epoch = OwnershipEpoch::new(2).unwrap();
    after.generation = RouteGeneration::new(2).unwrap();
    after.execution = ExecutionMode::Partitioned(
        (0..256u16)
            .map(|bucket| RouteEntry {
                scope: source_fixture::range(bucket, bucket + 1),
                target: RouteTarget::Group(group(1000 + u128::from(bucket))),
            })
            .collect(),
    );
    let intent = TransferIntent::new(before, ResponsibilityManifest::new(after).unwrap()).unwrap();
    let fence = OwnershipFence {
        group: group(20),
        responsibility: intent.before().input().responsibility,
        epoch: intent.before().input().epoch,
        operation: op(200),
        index: 4,
    };
    let exports = intent
        .targets()
        .iter()
        .map(|r| {
            let RouteTarget::Group(target) = r.target else {
                panic!("target")
            };
            SourceExportCommitment {
                target,
                scope: r.scope,
                digest: ContentDigest::sha256(&r.scope.start().to_le_bytes()),
            }
        })
        .collect::<Vec<_>>();
    let targets = exports
        .iter()
        .map(|e| TargetReadyEvidence {
            group: e.target,
            operation: op(200),
            configuration: ConfigurationId::new(1).unwrap(),
            staged_index: 1,
            imported: ImportStatus {
                index: 2,
                digest: ContentDigest::sha256(&e.digest.0),
                sources: vec![ImportedSource {
                    fence,
                    configuration: ConfigurationId::new(1).unwrap(),
                    scope: e.scope,
                    digest: e.digest,
                }],
            },
        })
        .collect();
    let source = SourceFenceEvidence {
        scope: None,
        fence,
        configuration: ConfigurationId::new(1).unwrap(),
        intent_digest: ContentDigest::sha256(&intent.encode(32768).unwrap()),
        exports,
    };
    let publication = TransferPublication::new(op(200), intent, vec![source], targets)
        .unwrap_or_else(|e| panic!("{:?}", e.0));
    let bytes = publication.encode(MAX_TRANSFER_PUBLICATION_BYTES).unwrap();
    assert_eq!(bytes.len(), 63624);
    assert!(bytes.len() <= voteboat::log::LogLimits::default().max_command_bytes);
    assert_eq!(TransferPublication::decode(&bytes).unwrap(), publication);
}

#[test]
fn maximum_256_source_merge_publication_fits_native_command_envelope() {
    use voteboat::{
        routed::OwnershipFence,
        transfer_source::SourceExportCommitment,
        transfer_target::{ImportStatus, ImportedSource},
    };
    let mut before = source_fixture::grant().into_input();
    before.execution = ExecutionMode::Partitioned(
        (0..256u16)
            .map(|bucket| RouteEntry {
                scope: source_fixture::range(bucket, bucket + 1),
                target: RouteTarget::Group(group(1000 + u128::from(bucket))),
            })
            .collect(),
    );
    let mut after = before.clone();
    after.execution = ExecutionMode::Single(group(20));
    after.epoch = OwnershipEpoch::new(2).unwrap();
    after.generation = RouteGeneration::new(2).unwrap();
    let intent = TransferIntent::new(
        ResponsibilityManifest::new(before).unwrap(),
        ResponsibilityManifest::new(after).unwrap(),
    )
    .unwrap();
    let intent_digest = ContentDigest::sha256(&intent.encode(32768).unwrap());
    let mut imports = Vec::new();
    let sources = intent
        .sources()
        .into_iter()
        .map(|route| {
            let RouteTarget::Group(source) = route.target else {
                panic!("source")
            };
            let fence = OwnershipFence {
                group: source,
                responsibility: intent.before().input().responsibility,
                epoch: intent.before().input().epoch,
                operation: op(200),
                index: 4,
            };
            let digest = ContentDigest::sha256(&route.scope.start().to_le_bytes());
            imports.push(ImportedSource {
                fence,
                configuration: ConfigurationId::new(1).unwrap(),
                scope: route.scope,
                digest,
            });
            SourceFenceEvidence {
                scope: None,
                fence,
                configuration: ConfigurationId::new(1).unwrap(),
                intent_digest,
                exports: vec![SourceExportCommitment {
                    target: group(20),
                    scope: route.scope,
                    digest,
                }],
            }
        })
        .collect();
    let target = TargetReadyEvidence {
        group: group(20),
        operation: op(200),
        configuration: ConfigurationId::new(1).unwrap(),
        staged_index: 1,
        imported: ImportStatus {
            index: 2,
            digest: ContentDigest::sha256(b"synthetic bounded metadata"),
            sources: imports,
        },
    };
    let publication = TransferPublication::new(op(200), intent, sources, vec![target])
        .unwrap_or_else(|e| panic!("{:?}", e.0));
    let bytes = publication.encode(MAX_TRANSFER_PUBLICATION_BYTES).unwrap();
    assert_eq!(bytes.len(), 61584);
    assert!(bytes.len() <= voteboat::log::LogLimits::default().max_command_bytes);
    assert_eq!(TransferPublication::decode(&bytes).unwrap(), publication);
}
