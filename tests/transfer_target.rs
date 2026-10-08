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
    application::*, bucket_counter::*, identity::*, routed::*, routing::*, transfer::ContentDigest,
    transfer_target::*,
};
#[path = "transfer_target/fixtures.rs"]
mod fixtures;
use fixtures::*;
use source::{entry, group, noop, op, range};
#[test]
fn stage_import_and_retries_keep_target_nonserving_and_source_lineage() {
    let mut t = fresh();
    let body = load();
    let log = [
        noop(1),
        entry(2, 200, t.bootstrap_command(65536).unwrap()),
        entry(3, 200, body.clone()),
        entry(4, 1, source::data(1, 7)),
        entry(5, 200, body.clone()),
    ];
    let bound = t.receipt_bytes_bound(&log).unwrap();
    let receipts = t.apply_batch(&log).unwrap();
    assert_eq!(
        bound,
        receipts.capacity() * std::mem::size_of::<TargetReceipt<BucketReceipt>>()
    );
    assert_eq!(receipts[0].outcome, TargetOutcome::Staged { index: 2 });
    let expected = TargetOutcome::Imported {
        index: 3,
        digest: ContentDigest::sha256(&body),
    };
    assert_eq!(receipts[1].outcome, expected);
    assert_eq!(receipts[2].outcome, TargetOutcome::NotActive);
    assert_eq!(receipts[3].outcome, expected);
    assert_eq!(t.application().value(&[1]), Ok(7));
    assert_eq!(t.application().outbox().count(), 1);
    let status = t.status();
    assert_eq!(status.staged_index, Some(2));
    let imported = status.imported.unwrap();
    assert_eq!(imported.index, 3);
    assert_eq!(imported.sources[0].fence.index, 4);
    assert_eq!(t.applied_index(), 5);
    assert_eq!(
        t.read_at(
            5,
            TargetQuery::Data(RoutedQuery {
                hint: source::hint(1),
                key: vec![1],
                query: vec![1]
            })
        )
        .unwrap(),
        TargetRead::NotActive
    );
    assert!(t
        .validate_proposal(op(3), &source::data(1, 1), std::iter::empty())
        .is_err());
    assert!(t
        .validate_proposal(op(200), &body, std::iter::empty())
        .is_ok());
    assert_eq!(
        t.apply_batch(&[entry(6, 200, t.bootstrap_command(65536).unwrap())])
            .unwrap()[0]
            .outcome,
        TargetOutcome::Staged { index: 2 }
    );
}
#[test]
fn canonical_import_codec_and_hash_bind_content_and_return_original_inputs() {
    assert_eq!(
        ContentDigest::sha256(b"abc").0,
        [
            0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae,
            0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61,
            0xf2, 0x00, 0x15, 0xad
        ]
    );
    let import = import();
    let bytes = import.encode(65536).unwrap();
    assert_eq!(TargetImport::decode(&bytes).unwrap(), import);
    assert!(import.encode(bytes.len() - 1).is_err());
    for end in 0..bytes.len() {
        assert!(TargetImport::decode(&bytes[..end]).is_err());
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(TargetImport::decode(&trailing).is_err());
    for change in 0..5 {
        let mut sources = import.sources().to_vec();
        match change {
            0 => sources[0].fence.index += 1,
            1 => sources[0].fence.operation = op(201),
            2 => sources[0].fence.group = group(99),
            3 => sources[0].fence.epoch = OwnershipEpoch::new(2).unwrap(),
            _ => {
                sources[0].fence.responsibility.incarnation =
                    ResponsibilityIncarnation::new(2).unwrap()
            }
        };
        let ptr = sources.as_ptr();
        let (error, _, returned) =
            TargetImport::new(op(200), source::intent(), group(21), sources).unwrap_err();
        assert_eq!(error, ApplicationError::InvalidCommand);
        assert_eq!(returned.as_ptr(), ptr);
    }
    assert!(TargetImport::new(op(200), source::intent(), group(21), vec![]).is_err());
    assert!(TargetImport::new(
        op(200),
        source::intent(),
        group(22),
        import.sources().to_vec()
    )
    .is_err());
    let mut spare = import.sources().to_vec();
    spare.reserve_exact(256);
    assert!(TargetImport::new(op(200), source::intent(), group(21), spare).is_err());
    let mut changed = bytes;
    *changed.last_mut().unwrap() ^= 1;
    assert_ne!(
        ContentDigest::sha256(&changed),
        ContentDigest::sha256(&import.encode(65536).unwrap())
    );
}
#[test]
fn conflicting_import_content_and_ids_cannot_replace_original_data() {
    let mut t = ready();
    let first = load();
    t.apply_batch(&[entry(2, 200, first.clone())]).unwrap();
    let original = t.status();
    let mut s = source::ready();
    s.apply_batch(&[
        entry(2, 1, source::data(1, 9)),
        entry(3, 2, source::data(200, 11)),
        entry(4, 200, source::freeze()),
    ])
    .unwrap();
    let different = t
        .import_command(
            &from_source(&s, 21, ConfigurationId::new(1).unwrap()),
            65536,
        )
        .unwrap();
    assert_ne!(
        ContentDigest::sha256(&first),
        ContentDigest::sha256(&different)
    );
    assert!(t
        .validate_proposal(op(200), &different, std::iter::empty())
        .is_err());
    assert_eq!(
        t.apply_batch(&[entry(3, 200, different)]).unwrap()[0].outcome,
        TargetOutcome::OperationConflict
    );
    assert_eq!(
        t.apply_batch(&[entry(4, 201, first)]).unwrap()[0].outcome,
        TargetOutcome::OperationConflict
    );
    assert_eq!(t.status(), original);
    assert_eq!(t.application().value(&[1]), Ok(7));
    assert_eq!(t.application().outbox().count(), 1);
}
#[test]
fn checkpoint_and_wal_replay_restore_original_import_status_and_content() {
    let log = [
        noop(1),
        entry(2, 200, fresh().bootstrap_command(65536).unwrap()),
        entry(3, 200, load()),
        noop(4),
        entry(5, 200, load()),
    ];
    let mut t = fresh();
    t.apply_batch(&log).unwrap();
    let bytes = t.checkpoint(100000).unwrap();
    assert_eq!(bytes.len(), t.checkpoint(bytes.len()).unwrap().len());
    assert!(t.checkpoint(bytes.len() - 1).is_err());
    let mut r = fresh();
    r.restore_checkpoint(1, 5, &bytes).unwrap();
    assert_eq!(r.checkpoint(100000).unwrap(), bytes);
    assert_eq!(r.status(), t.status());
    r.apply_batch(&[noop(6), entry(7, 200, load())]).unwrap();
    assert_eq!(r.status(), t.status());
    assert_eq!(r.application().value(&[1]), Ok(7));
    let mut replay = fresh();
    replay.apply_batch(&log).unwrap();
    assert_eq!(replay.checkpoint(100000).unwrap(), bytes);
    for end in 0..bytes.len() {
        let mut r = fresh();
        let old = r.checkpoint(100000).unwrap();
        assert!(r.restore_checkpoint(1, 5, &bytes[..end]).is_err());
        assert_eq!(r.checkpoint(100000).unwrap(), old);
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(fresh().restore_checkpoint(1, 5, &trailing).is_err());
    assert!(fresh().restore_checkpoint(2, 5, &bytes).is_err());
    assert!(fresh().restore_checkpoint(1, 4, &bytes).is_err());
    let mut changed = TransferTarget::new(
        group(21),
        op(201),
        source::intent(),
        BucketCounter::new(range(0, 128), source::Policy, source::bucket_limits()).unwrap(),
        source::Policy,
        limits(),
    )
    .unwrap_or_else(|e| panic!("{:?}", e.0));
    assert!(changed.restore_checkpoint(1, 5, &bytes).is_err());
}
#[test]
fn admission_and_failed_batches_refuse_missing_stage_bad_data_and_provider_capacity() {
    let mut t = fresh();
    let original = t.checkpoint(100000).unwrap();
    assert!(t
        .validate_proposal(op(200), &load(), std::iter::empty())
        .is_err());
    assert!(t.apply_batch(&[entry(1, 200, load())]).is_err());
    assert_eq!(t.checkpoint(100000).unwrap(), original);
    let boot = t.bootstrap_command(65536).unwrap();
    assert!(t
        .validate_proposal(op(200), &load(), [(op(200), boot.as_slice())].into_iter())
        .is_ok());
    assert!(t
        .validate_proposal(
            op(200),
            &load(),
            std::iter::repeat_n((op(200), boot.as_slice()), MAX_ROUTED_PENDING + 1)
        )
        .is_err());
    let oversized = vec![0; t.readiness_requirements().command_bytes + 1];
    assert!(t
        .validate_proposal(
            op(200),
            &load(),
            [(op(200), oversized.as_slice())].into_iter()
        )
        .is_err());
    let mut malformed = load();
    malformed[8] ^= 1;
    assert!(t
        .apply_batch(&[entry(1, 200, boot), entry(2, 200, malformed)])
        .is_err());
    assert_eq!(t.checkpoint(100000).unwrap(), original);
    let mut tiny = TransferTarget::new(
        group(21),
        op(200),
        source::intent(),
        BucketCounter::new(
            range(0, 128),
            source::Policy,
            BucketCounterLimits {
                operations: 1,
                semantic_bytes: 1,
            },
        )
        .unwrap(),
        source::Policy,
        limits(),
    )
    .unwrap_or_else(|e| panic!("{:?}", e.0));
    tiny.apply_batch(&[entry(1, 200, tiny.bootstrap_command(65536).unwrap())])
        .unwrap();
    let bytes = tiny.import_command(&import(), 65536).unwrap();
    let before = tiny.checkpoint(100000).unwrap();
    assert!(tiny
        .validate_proposal(op(200), &bytes, std::iter::empty())
        .is_err());
    assert!(tiny.apply_batch(&[entry(2, 200, bytes)]).is_err());
    assert_eq!(tiny.checkpoint(100000).unwrap(), before);
    let mut t = ready();
    let bytes = load();
    let before = t.checkpoint(100000).unwrap();
    for end in 0..bytes.len() {
        assert!(t
            .apply_batch(&[entry(2, 200, bytes[..end].to_vec())])
            .is_err());
        assert_eq!(t.checkpoint(100000).unwrap(), before);
    }
}
#[test]
fn target_status_reads_are_prefix_checked_and_charge_spare_source_capacity() {
    let mut t = ready();
    t.apply_batch(&[entry(2, 200, load())]).unwrap();
    assert!(t.read_at(3, TargetQuery::Status).is_err());
    let mut result = t.read_at(2, TargetQuery::Status).unwrap();
    let nested = t.read_result_bytes(&result, 65536).unwrap();
    assert_eq!(
        t.read_result_bound(&TargetQuery::Status).unwrap(),
        std::mem::size_of_val(&result) + nested
    );
    assert!(t.read_result_bytes(&result, nested - 1).is_err());
    let TargetRead::Status(s) = &mut result else {
        panic!("status")
    };
    s.imported.as_mut().unwrap().sources.reserve_exact(10);
    assert!(t.read_result_bytes(&result, nested).is_err());
    let query = TargetQuery::Data(RoutedQuery {
        hint: source::hint(1),
        key: Vec::with_capacity(MAX_ROUTING_KEY_BYTES + 1),
        query: vec![1],
    });
    assert!(t.query_bytes(&query, usize::MAX).is_err());
}
#[test]
fn constructor_and_binding_refuse_wrong_target_scope_limits_and_keep_provider() {
    let app = BucketCounter::new(range(0, 128), source::Policy, source::bucket_limits()).unwrap();
    let initial = app.checkpoint(65536).unwrap();
    let (_, _, returned, _) = match TransferTarget::new(
        group(22),
        op(200),
        source::intent(),
        app,
        source::Policy,
        limits(),
    ) {
        Ok(_) => panic!("scope"),
        Err(e) => e,
    };
    assert_eq!(returned.checkpoint(65536).unwrap(), initial);
    let mut l = limits();
    l.import_bytes = 0;
    assert!(TransferTarget::new(
        group(21),
        op(200),
        source::intent(),
        returned,
        source::Policy,
        l
    )
    .is_err());
    let t = fresh();
    assert_eq!(
        t.validate_group(group(22)),
        Err(ApplicationError::InvalidCommand)
    );
    assert!(t.bootstrap_command(1).is_err());
    assert!(t.import_command(&import(), 1).is_err());
    let bytes = load();
    let mut changed = bytes;
    changed[8] ^= 1;
    assert!(ready()
        .validate_proposal(op(200), &changed, std::iter::empty())
        .is_err());
}

#[path = "transfer_source/fixtures.rs"]
pub mod source_fixture;

#[test]
fn expected_source_commitment_refuses_altered_image_content_or_metadata() {
    let original = import();
    for metadata in [false, true] {
        let mut sources = original.sources().to_vec();
        let image = &sources[0].image;
        let mut bytes = image.bytes().to_vec();
        if !metadata {
            *bytes.last_mut().unwrap() ^= 1;
        }
        sources[0].image = voteboat::scope::ScopeImage::new(
            if metadata {
                image.schema() + 1
            } else {
                image.schema()
            },
            image.scheme(),
            image.scope(),
            image.source_applied(),
            bytes,
        )
        .unwrap();
        assert!(TargetImport::new(op(200), source::intent(), group(21), sources).is_err());
    }
    let status = frozen()
        .read_at(4, voteboat::transfer_source::SourceQuery::Freeze)
        .unwrap();
    let voteboat::transfer_source::SourceRead::Freeze(Some(status)) = status else {
        panic!("source status")
    };
    let expected = status
        .exports
        .iter()
        .find(|e| e.target == group(21))
        .unwrap();
    assert_eq!(expected.digest, original.sources()[0].digest);
    assert_eq!(expected.scope, original.sources()[0].image.scope());
}

#[test]
fn multi_source_import_requires_ordered_complete_coverage_and_refuses_id_collisions() {
    use voteboat::{transfer::*, transfer_source::*};
    let mut before = source::grant().into_input();
    before.execution = ExecutionMode::Partitioned(vec![
        RouteEntry {
            scope: range(0, 128),
            target: RouteTarget::Group(group(21)),
        },
        RouteEntry {
            scope: range(128, 256),
            target: RouteTarget::Group(group(22)),
        },
    ]);
    let before = ResponsibilityManifest::new(before).unwrap();
    let mut after = before.clone().into_input();
    after.epoch = OwnershipEpoch::new(2).unwrap();
    after.generation = RouteGeneration::new(2).unwrap();
    after.execution = ExecutionMode::Single(group(23));
    let intent =
        TransferIntent::new(before.clone(), ResponsibilityManifest::new(after).unwrap()).unwrap();
    for collision in [false, true] {
        let mut images = Vec::new();
        for (g, scope, key, id, delta) in [
            (21, range(0, 128), 1u8, 1u128, 7i64),
            (
                22,
                range(128, 256),
                200u8,
                if collision { 1 } else { 2 },
                11i64,
            ),
        ] {
            let routed = RoutedApplication::new(
                group(g),
                before.clone(),
                BucketCounter::new(scope, source::Policy, source::bucket_limits()).unwrap(),
                source::Policy,
                RoutedLimits {
                    operations: 32,
                    semantic_bytes: 8192,
                    payload_bytes: 1024,
                    inner_checkpoint_bytes: source::bucket_limits().checkpoint_bound().unwrap(),
                },
            )
            .unwrap_or_else(|e| panic!("{:?}", e.error));
            let mut source =
                TransferSource::new(routed, 65536).unwrap_or_else(|e| panic!("{:?}", e.0));
            let mut hint = source::hint(key);
            hint.group = group(g);
            hint.scope = scope;
            let bytes = encode_routed(
                hint,
                &[key],
                &encode_add(&[key], delta, b"merge-effect", 1024).unwrap(),
                4096,
            )
            .unwrap();
            source
                .apply_batch(&[
                    entry(1, 100, source.bootstrap_command(65536).unwrap()),
                    entry(2, id, bytes),
                    entry(
                        3,
                        200,
                        source::Source::freeze_command(&intent, 32776).unwrap(),
                    ),
                ])
                .unwrap();
            let image = source.export_target(group(23), 65536).unwrap();
            let SourceRead::Freeze(Some(status)) = source.read_at(3, SourceQuery::Freeze).unwrap()
            else {
                panic!("freeze status")
            };
            images.push(SourceImport {
                fence: status.fence,
                configuration: ConfigurationId::new(1).unwrap(),
                image,
                digest: status.exports[0].digest,
            });
        }
        assert!(
            TargetImport::new(op(200), intent.clone(), group(23), images[..1].to_vec()).is_err()
        );
        let mut reversed = images.clone();
        reversed.reverse();
        assert!(TargetImport::new(op(200), intent.clone(), group(23), reversed).is_err());
        let import = TargetImport::new(op(200), intent.clone(), group(23), images)
            .unwrap_or_else(|e| panic!("{:?}", e.0));
        let mut target = TransferTarget::new(
            group(23),
            op(200),
            intent.clone(),
            BucketCounter::new(range(0, 256), source::Policy, source::bucket_limits()).unwrap(),
            source::Policy,
            limits(),
        )
        .unwrap_or_else(|e| panic!("{:?}", e.0));
        target
            .apply_batch(&[entry(1, 200, target.bootstrap_command(65536).unwrap())])
            .unwrap();
        let old = target.checkpoint(100000).unwrap();
        let command = target.import_command(&import, 65536).unwrap();
        if collision {
            assert!(target
                .validate_proposal(op(200), &command, std::iter::empty())
                .is_err());
            assert!(target.apply_batch(&[entry(2, 200, command)]).is_err());
            assert_eq!(target.checkpoint(100000).unwrap(), old);
        } else {
            target.apply_batch(&[entry(2, 200, command)]).unwrap();
            assert_eq!(target.application().value(&[1]), Ok(7));
            assert_eq!(target.application().value(&[200]), Ok(11));
            assert_eq!(target.application().outbox().count(), 2);
            assert_eq!(target.status().imported.unwrap().sources.len(), 2);
        }
    }
}
