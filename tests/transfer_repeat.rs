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
    application::*, bucket_counter::*, identity::*, routed::*, routing::*, scope::*, transfer::*,
    transfer_publication::*, transfer_source::*, transfer_target::*,
};
#[path = "transfer_target/fixtures.rs"]
mod fixture;
#[path = "transfer_source/fixtures.rs"]
pub mod source_fixture;
use source_fixture::{entry, group, noop, op, range};

fn cfg() -> ConfigurationId {
    ConfigurationId::new(1).unwrap()
}
fn targets(intent: &TransferIntent, operation: u128) -> Vec<fixture::Target> {
    intent
        .targets()
        .iter()
        .map(|route| {
            let RouteTarget::Group(g) = route.target else {
                panic!("concrete owner")
            };
            TransferTarget::new(
                g,
                op(operation),
                intent.clone(),
                BucketCounter::new(
                    route.scope,
                    source_fixture::Policy,
                    source_fixture::bucket_limits(),
                )
                .unwrap(),
                source_fixture::Policy,
                fixture::limits(),
            )
            .unwrap_or_else(|e| panic!("{:?}", e.0))
        })
        .collect()
}
fn active_split() -> Vec<fixture::Target> {
    let source = fixture::frozen();
    let mut targets = targets(&source_fixture::intent(), 200);
    for (t, g) in targets.iter_mut().zip([21, 22]) {
        let import = fixture::from_source(&source, g, cfg());
        t.apply_batch(&[
            entry(1, 200, t.bootstrap_command(65536).unwrap()),
            entry(2, 200, t.import_command(&import, 65536).unwrap()),
        ])
        .unwrap();
        let activation = TargetActivation {
            metadata_configuration: cfg(),
            decision: TransferPublicationStatus {
                publication_operation: op(201),
                index: 4,
                publication: fixture::publication(),
            },
        };
        t.apply_batch(&[entry(
            3,
            200,
            t.activation_command(&activation, 65536).unwrap(),
        )])
        .unwrap();
    }
    targets
}
fn next_intent(before: ResponsibilityManifest, execution: ExecutionMode) -> TransferIntent {
    let mut after = before.clone().into_input();
    after.epoch = OwnershipEpoch::new(after.epoch.get() + 1).unwrap();
    after.generation = RouteGeneration::new(after.generation.get() + 1).unwrap();
    after.execution = execution;
    TransferIntent::new(before, ResponsibilityManifest::new(after).unwrap()).unwrap()
}
fn merge_intent() -> TransferIntent {
    next_intent(
        source_fixture::intent().after().clone(),
        ExecutionMode::Single(group(23)),
    )
}
fn hint(manifest: &ResponsibilityManifest, key: u8) -> RouteHint {
    let input = manifest.input();
    let (g, scope) = match &input.execution {
        ExecutionMode::Single(g) => (*g, input.scope),
        ExecutionMode::Partitioned(routes) => {
            let r = routes
                .iter()
                .find(|r| r.scope.start() <= key as u16 && (key as u16) < r.scope.end())
                .unwrap();
            let RouteTarget::Group(g) = r.target else {
                panic!("owner")
            };
            (g, r.scope)
        }
        _ => panic!("concrete"),
    };
    let mut h = source_fixture::hint(key);
    h.group = g;
    h.scope = scope;
    h.epoch = input.epoch;
    h.generation = input.generation;
    h
}
fn data(manifest: &ResponsibilityManifest, key: u8, delta: i64) -> Vec<u8> {
    encode_routed(
        hint(manifest, key),
        &[key],
        &encode_add(&[key], delta, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap()
}
fn query(manifest: &ResponsibilityManifest, key: u8) -> TargetQuery<Vec<u8>> {
    TargetQuery::Data(RoutedQuery {
        hint: hint(manifest, key),
        key: vec![key],
        query: vec![key],
    })
}
fn freeze_status(t: &fixture::Target) -> SourceFreezeStatus {
    let TargetRead::Freeze(Some(s)) = t.read_at(t.applied_index(), TargetQuery::Freeze).unwrap()
    else {
        panic!("frozen")
    };
    let q = TargetQuery::Freeze;
    assert!(
        t.read_result_bytes(&TargetRead::Freeze(Some(s.clone())), usize::MAX)
            .unwrap()
            + std::mem::size_of::<TargetRead<i64>>()
            <= t.read_result_bound(&q).unwrap()
    );
    s
}
fn handoff(
    sources: &mut [fixture::Target],
    intent: &TransferIntent,
    operation: u128,
) -> Vec<fixture::Target> {
    let mut targets = targets(intent, operation);
    for t in &mut targets {
        t.apply_batch(&[entry(1, operation, t.bootstrap_command(65536).unwrap())])
            .unwrap();
    }
    // Preflight every source before committing any irreversible fence.
    let commands: Vec<_> = sources
        .iter()
        .map(|s| {
            let bytes = s.freeze_command(intent, 65536, 65536).unwrap();
            s.validate_proposal(op(operation), &bytes, std::iter::empty())
                .unwrap();
            bytes
        })
        .collect();
    for (source, bytes) in sources.iter_mut().zip(commands) {
        freeze_source(source, intent, operation, bytes);
    }
    let statuses: Vec<_> = sources.iter().map(freeze_status).collect();
    for target in &mut targets {
        let g = target.status().group;
        let imports = sources
            .iter()
            .zip(&statuses)
            .filter_map(|(s, status)| {
                status
                    .exports
                    .iter()
                    .find(|e| e.target == g)
                    .map(|e| SourceImport {
                        fence: status.fence,
                        configuration: cfg(),
                        image: s.export_target(g, 65536).unwrap(),
                        digest: e.digest,
                    })
            })
            .collect();
        let import = TargetImport::new(op(operation), intent.clone(), g, imports)
            .unwrap_or_else(|e| panic!("{:?}", e.0));
        target
            .apply_batch(&[entry(
                2,
                operation,
                target.import_command(&import, 65536).unwrap(),
            )])
            .unwrap();
        assert_eq!(
            target
                .read_at(
                    2,
                    query(intent.after(), target.application().scope().start() as u8)
                )
                .unwrap(),
            TargetRead::NotActive
        );
    }
    let source_evidence = statuses
        .into_iter()
        .map(|s| SourceFenceEvidence::from_status(cfg(), s).unwrap_or_else(|e| panic!("{:?}", e.0)))
        .collect();
    let target_evidence = targets
        .iter()
        .map(|t| {
            TargetReadyEvidence::from_status(cfg(), t.status())
                .unwrap_or_else(|e| panic!("{:?}", e.0))
        })
        .collect();
    let publication = TransferPublication::new(
        op(operation),
        intent.clone(),
        source_evidence,
        target_evidence,
    )
    .unwrap_or_else(|e| panic!("{:?}", e.0));
    let activation = TargetActivation {
        metadata_configuration: cfg(),
        decision: TransferPublicationStatus {
            publication_operation: op(operation + 1),
            index: 7,
            publication,
        },
    };
    for t in &mut targets {
        t.apply_batch(&[entry(
            3,
            operation,
            t.activation_command(&activation, 65536).unwrap(),
        )])
        .unwrap();
    }
    targets
}
fn applied(
    t: &mut fixture::Target,
    manifest: &ResponsibilityManifest,
    id: u128,
    key: u8,
    delta: i64,
    value: i64,
    duplicate: bool,
) {
    let bytes = data(manifest, key, delta);
    t.validate_proposal(op(id), &bytes, std::iter::empty())
        .unwrap();
    let r = t
        .apply_batch(&[entry(t.applied_index() + 1, id, bytes)])
        .unwrap()
        .remove(0);
    let TargetOutcome::Applied(r) = r.outcome else {
        panic!("data")
    };
    assert_eq!(r.outcome, BucketOutcome::Value(value));
    assert_eq!(r.duplicate, duplicate);
}
#[test]
fn activated_split_targets_merge_and_split_again_with_original_retries_and_outbox() {
    let mut split = active_split();
    applied(
        &mut split[0],
        source_fixture::intent().after(),
        3,
        1,
        2,
        9,
        false,
    );
    let merge = merge_intent();
    let mut merged = handoff(&mut split, &merge, 300);
    applied(&mut merged[0], merge.after(), 1, 1, 7, 7, true);
    applied(&mut merged[0], merge.after(), 3, 1, 2, 9, true);
    applied(&mut merged[0], merge.after(), 4, 200, 3, 14, false);
    let next = next_intent(
        merge.after().clone(),
        ExecutionMode::Partitioned(vec![
            RouteEntry {
                scope: range(0, 128),
                target: RouteTarget::Group(group(24)),
            },
            RouteEntry {
                scope: range(128, 256),
                target: RouteTarget::Group(group(25)),
            },
        ]),
    );
    let mut final_targets = handoff(&mut merged, &next, 400);
    applied(&mut final_targets[0], next.after(), 1, 1, 7, 7, true);
    applied(&mut final_targets[0], next.after(), 3, 1, 2, 9, true);
    applied(&mut final_targets[1], next.after(), 2, 200, 11, 11, true);
    applied(&mut final_targets[1], next.after(), 4, 200, 3, 14, true);
    applied(&mut final_targets[0], next.after(), 5, 1, 1, 10, false);
    applied(&mut final_targets[1], next.after(), 6, 200, 2, 16, false);
    for (t, key, value) in [(&final_targets[0], 1, 10), (&final_targets[1], 200, 16)] {
        assert_eq!(
            t.read_at(t.applied_index(), query(next.after(), key))
                .unwrap(),
            TargetRead::Data(value)
        );
        assert_eq!(t.application().outbox().count(), 3);
        let cp = t.checkpoint(300000).unwrap();
        let mut r = targets(&next, 400)
            .into_iter()
            .find(|r| r.status().group == t.status().group)
            .unwrap();
        r.restore_checkpoint(TRANSFER_TARGET_SCHEMA, t.applied_index(), &cp)
            .unwrap();
        assert_eq!(r.status(), t.status());
        assert_eq!(
            r.application().checkpoint(65536).unwrap(),
            t.application().checkpoint(65536).unwrap()
        );
    }
    for s in split.iter().chain(merged.iter()) {
        let key = s.application().scope().start() as u8;
        assert_eq!(
            s.read_at(
                s.applied_index(),
                query(&freeze_status(s).intent.before().clone(), key)
            )
            .unwrap(),
            TargetRead::Rejected(RoutingError::Fenced)
        );
    }
}
#[test]
fn later_freeze_checks_activation_binding_manifest_budget_and_reserved_operation_ids() {
    let merge = merge_intent();
    let mut s = active_split().remove(0);
    let bytes = s.freeze_command(&merge, 65536, 65536).unwrap();
    assert!(fixture::fresh()
        .freeze_command(&merge, 65536, 65536)
        .is_err());
    assert!(s
        .freeze_command(&source_fixture::intent(), 65536, 65536)
        .is_err());
    for budget in [0, 1, MAX_SCOPE_IMAGE_BYTES + 1] {
        assert!(s.freeze_command(&merge, budget, 65536).is_err());
    }
    assert!(s.freeze_command(&merge, 65536, bytes.len() - 1).is_err());
    let before = s.checkpoint(300000).unwrap();
    for id in [1, 200] {
        assert!(s
            .validate_proposal(op(id), &bytes, std::iter::empty())
            .is_err());
        let mut rejected = s.clone();
        assert_eq!(
            rejected
                .apply_batch(&[entry(4, id, bytes.clone())])
                .unwrap()[0]
                .outcome,
            TargetOutcome::OperationConflict
        );
        assert!(rejected.fence().is_none());
        assert!(rejected.application().contains_operation(op(1)));
    }
    for end in 0..bytes.len() {
        assert!(s
            .apply_batch(&[entry(4, 300, bytes[..end].to_vec())])
            .is_err());
        assert_eq!(s.checkpoint(300000).unwrap(), before);
    }
    let mut foreign = bytes.clone();
    foreign[8] ^= 1;
    assert!(s.apply_batch(&[entry(4, 300, foreign)]).is_err());
    assert_eq!(s.checkpoint(300000).unwrap(), before);
    check_pending_activation(&bytes);
    s.apply_batch(&[entry(4, 300, bytes.clone())]).unwrap();
    let frozen = s.checkpoint(300000).unwrap();
    let status = freeze_status(&s);
    let mut changed = bytes.clone();
    changed[40..48].copy_from_slice(&65535u64.to_le_bytes());
    for (id, command) in [
        (301, bytes.clone()),
        (300, changed),
        (9, data(merge.before(), 1, 1)),
    ] {
        assert!(s
            .validate_proposal(op(id), &command, std::iter::empty())
            .is_err());
    }
    assert!(s
        .validate_proposal(op(300), &bytes, std::iter::empty())
        .is_ok());
    assert!(s
        .validate_proposal(
            op(9),
            &data(merge.before(), 1, 1),
            [(op(300), bytes.as_slice())].into_iter()
        )
        .is_err());
    assert_eq!(s.checkpoint(300000).unwrap(), frozen);
    check_truncated_frozen_checkpoint(&frozen);
    let mut replay = active_split().remove(0);
    replay
        .apply_batch(&[entry(4, 300, bytes), noop(5)])
        .unwrap();
    assert_eq!(freeze_status(&replay), status);
    assert_eq!(replay.application().applied_index(), 4);
    assert_eq!(replay.applied_index(), 5);
    assert!(replay.read_at(6, TargetQuery::Freeze).is_err());
}
#[test]
fn import_cannot_hide_an_original_data_id_behind_its_lifecycle_id() {
    let source = fixture::frozen();
    // The import is structurally valid but its lifecycle ID equals retained op 1.
    let import = fixture::from_source(&source, 21, cfg());
    let mut images = import.sources().to_vec();
    for i in &mut images {
        i.fence.operation = op(1);
    }
    let import = TargetImport::new(op(1), source_fixture::intent(), group(21), images)
        .unwrap_or_else(|e| panic!("{:?}", e.0));
    let mut t = targets(&source_fixture::intent(), 1).remove(0);
    t.apply_batch(&[entry(1, 1, t.bootstrap_command(65536).unwrap())])
        .unwrap();
    let bytes = t.import_command(&import, 65536).unwrap();
    let before = t.checkpoint(300000).unwrap();
    assert!(t
        .validate_proposal(op(1), &bytes, std::iter::empty())
        .is_err());
    assert!(t.apply_batch(&[entry(2, 1, bytes)]).is_err());
    assert_eq!(t.checkpoint(300000).unwrap(), before);
    assert!(t.status().imported.is_none());
    assert!(t.status().activated.is_none());
}

#[test]
fn full_provider_history_reserves_freeze_without_consuming_a_data_slot() {
    let mut source = active_split().remove(0);
    for id in 2..=32 {
        applied(
            &mut source,
            source_fixture::intent().after(),
            id,
            1,
            0,
            7,
            false,
        );
    }
    assert!(!source.application().contains_operation(op(300)));
    let command = source
        .freeze_command(&merge_intent(), 65536, 65536)
        .unwrap();
    source
        .validate_proposal(op(300), &command, std::iter::empty())
        .unwrap();
    let index = source.applied_index() + 1;
    source.apply_batch(&[entry(index, 300, command)]).unwrap();
    assert_eq!(source.application().outbox().count(), 32);
    assert!(!source.application().contains_operation(op(300)));
    assert_eq!(freeze_status(&source).fence.index, index);
}

#[test]
fn frozen_checkpoint_rejects_changed_boundaries_operation_and_bootstrap_atomically() {
    let mut source = active_split().remove(0);
    let command = source
        .freeze_command(&merge_intent(), 65536, 65536)
        .unwrap();
    source
        .apply_batch(&[entry(4, 300, command), noop(5)])
        .unwrap();
    let checkpoint = source.checkpoint(300000).unwrap();
    let word =
        |offset| u32::from_le_bytes(checkpoint[offset..offset + 4].try_into().unwrap()) as usize;
    let binding = word(16);
    let load_offset = 20 + binding + 16;
    let activation_offset = load_offset + 4 + word(load_offset);
    let boundary_offset = activation_offset + 12 + word(activation_offset + 8);
    let mut changes = Vec::new();
    for (offset, bytes) in [
        (boundary_offset, 5u64.to_le_bytes().to_vec()),
        (boundary_offset + 24, 0u64.to_le_bytes().to_vec()),
        (boundary_offset + 8, 1u128.to_le_bytes().to_vec()),
        (boundary_offset + 8, 200u128.to_le_bytes().to_vec()),
        (boundary_offset + 8, 0u128.to_le_bytes().to_vec()),
    ] {
        let mut changed = checkpoint.clone();
        changed[offset..offset + bytes.len()].copy_from_slice(&bytes);
        changes.push(changed);
    }
    let mut changed = checkpoint.clone();
    changed[boundary_offset + 36 + 8] ^= 1;
    changes.push(changed);
    for changed in changes {
        let mut recovered = fixture::fresh();
        let before = recovered.checkpoint(300000).unwrap();
        assert!(recovered
            .restore_checkpoint(TRANSFER_TARGET_SCHEMA, 5, &changed)
            .is_err());
        assert_eq!(recovered.checkpoint(300000).unwrap(), before);
    }
}

fn freeze_source(
    source: &mut fixture::Target,
    intent: &TransferIntent,
    operation: u128,
    bytes: Vec<u8>,
) {
    let original = source.status();
    let f = source.applied_index() + 1;
    let entries = [
        entry(f, operation, bytes.clone()),
        noop(f + 1),
        entry(
            f + 2,
            999,
            data(
                intent.before(),
                source.application().scope().start() as u8,
                1,
            ),
        ),
        entry(f + 3, operation, bytes),
    ];
    let bound = source.receipt_bytes_bound(&entries).unwrap();
    let receipts = source.apply_batch(&entries).unwrap();
    assert_eq!(
        bound,
        receipts.len() * std::mem::size_of::<TargetReceipt<BucketReceipt>>()
    );
    assert_eq!(
        receipts[0].outcome,
        TargetOutcome::Frozen(source.fence().unwrap())
    );
    assert_eq!(
        receipts[1].outcome,
        TargetOutcome::Rejected(RoutingError::Fenced)
    );
    assert_eq!(receipts[2].outcome, receipts[0].outcome);
    assert_eq!(source.status(), original);
    assert_eq!(source.application().applied_index(), f);
    assert_eq!(source.applied_index(), f + 3);
    let cp = source.checkpoint(300000).unwrap();
    let mut restored = source.clone();
    restored
        .restore_checkpoint(TRANSFER_TARGET_SCHEMA, f + 3, &cp)
        .unwrap();
    assert_eq!(
        restored.freeze_status().unwrap(),
        source.freeze_status().unwrap()
    );
    assert_eq!(
        restored.application().checkpoint(65536).unwrap(),
        source.application().checkpoint(65536).unwrap()
    );
    *source = restored;
}

fn check_pending_activation(bytes: &[u8]) {
    // Pending activation followed by freeze is evaluated against the staged copy.
    let mut pending = fixture::ready();
    pending
        .apply_batch(&[entry(2, 200, fixture::load())])
        .unwrap();
    let activation = TargetActivation {
        metadata_configuration: cfg(),
        decision: TransferPublicationStatus {
            publication_operation: op(201),
            index: 4,
            publication: fixture::publication(),
        },
    };
    let activate = pending.activation_command(&activation, 65536).unwrap();
    assert!(pending
        .validate_proposal(op(300), bytes, [(op(200), activate.as_slice())].into_iter())
        .is_ok());
    assert_eq!(pending.fence(), None);
}

fn check_truncated_frozen_checkpoint(frozen: &[u8]) {
    for end in 0..frozen.len() {
        let mut r = fixture::fresh();
        let pristine = r.checkpoint(300000).unwrap();
        assert!(r
            .restore_checkpoint(TRANSFER_TARGET_SCHEMA, 4, &frozen[..end])
            .is_err());
        assert_eq!(r.checkpoint(300000).unwrap(), pristine);
    }
}
