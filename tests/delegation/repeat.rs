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
use voteboat::{scope::*, transfer_publication::*};
type Target = TransferTarget<BucketCounter<base::Policy>, base::Policy>;
fn cfg() -> ConfigurationId {
    ConfigurationId::new(1).unwrap()
}
fn current(d: &LifecycleDirectory, id: ResponsibilityIdentity) -> ResponsibilityManifest {
    let DirectoryRead::Manifest(Some(m)) = d
        .read_at(d.applied_index(), DirectoryQuery::Manifest(id))
        .unwrap()
    else {
        panic!("manifest")
    };
    m
}
fn recover_target(t: &mut Target, child: &LifecycleDirectory) {
    let bytes = t.checkpoint(300000).unwrap();
    let status = t.status();
    let DirectoryRead::Transfer(Some(original)) = child
        .read_at(
            child.applied_index(),
            DirectoryQuery::Transfer(status.operation),
        )
        .unwrap()
    else {
        panic!("original intent")
    };
    let mut recovered = make_targets(&original.intent, status.operation.get())
        .into_iter()
        .find(|fresh| fresh.status().group == status.group)
        .unwrap();
    assert_eq!(recovered.applied_index(), 0);
    recovered
        .restore_checkpoint(t.schema_version(), t.applied_index(), &bytes)
        .unwrap();
    assert_eq!(recovered.checkpoint(300000).unwrap(), bytes);
    *t = recovered;
}
fn recover_journal(d: &mut LifecycleDirectory, parent: bool) {
    *d = recover_directory(d, parent, 10);
}
fn make_targets(intent: &TransferIntent, operation: u128) -> Vec<Target> {
    intent
        .targets()
        .iter()
        .map(|r| {
            let RouteTarget::Group(g) = r.target else {
                panic!("owner")
            };
            TransferTarget::new(
                g,
                op(operation),
                intent.clone(),
                BucketCounter::new(r.scope, base::Policy, base::bucket_limits()).unwrap(),
                base::Policy,
                TargetLimits {
                    import_bytes: 65536,
                    application_checkpoint_bytes: base::bucket_limits().checkpoint_bound().unwrap(),
                },
            )
            .unwrap_or_else(|e| panic!("{:?}", e.0))
        })
        .collect()
}
fn route(manifest: &ResponsibilityManifest, key: u8) -> RouteHint {
    let cache = Cache(BTreeMap::from([(id(10), manifest.clone())]));
    resolve(&cache, &base::Policy, id(10), &[key], 1).unwrap()
}
fn data(manifest: &ResponsibilityManifest, key: u8, delta: i64) -> Vec<u8> {
    encode_routed(
        route(manifest, key),
        &[key],
        &encode_add(&[key], delta, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap()
}
fn query(manifest: &ResponsibilityManifest, key: u8) -> TargetQuery<Vec<u8>> {
    TargetQuery::Data(RoutedQuery {
        hint: route(manifest, key),
        key: vec![key],
        query: vec![key],
    })
}
fn write(
    t: &mut Target,
    m: &ResponsibilityManifest,
    operation: u128,
    key: u8,
    delta: i64,
    value: i64,
    duplicate: bool,
) {
    let bytes = data(m, key, delta);
    t.validate_proposal(op(operation), &bytes, std::iter::empty())
        .unwrap();
    let TargetOutcome::Applied(r) = command(t, operation, bytes).outcome else {
        panic!("write")
    };
    assert_eq!(r.outcome, BucketOutcome::Value(value));
    assert_eq!(r.duplicate, duplicate);
}
fn reserve(
    p: &mut LifecycleDirectory,
    c: &mut LifecycleDirectory,
    operation: u128,
    execution: ExecutionMode,
) -> (DelegationReservationStatus, TransferIntent) {
    let before = current(c, id(10));
    let mut after = before.clone().into_input();
    after.epoch = OwnershipEpoch::new(after.epoch.get() + 1).unwrap();
    after.generation = RouteGeneration::new(after.generation.get() + 1).unwrap();
    after.execution = execution;
    let plan = DelegationPlan::new(
        current(p, id(500)),
        before,
        ResponsibilityManifest::new(after).unwrap(),
        op(operation),
    )
    .unwrap();
    assert_eq!(
        command(p, operation + 200, plan.encode(65536).unwrap()).outcome,
        DirectoryOutcome::DelegationReserved
    );
    recover_journal(p, true);
    let DirectoryRead::DelegationReservation(Some(s)) = p
        .read_at(
            p.applied_index(),
            DirectoryQuery::DelegationReservation(op(operation + 200)),
        )
        .unwrap()
    else {
        panic!("reservation")
    };
    let intent = s.child_intent(cfg()).unwrap();
    assert_eq!(
        command(c, operation, intent.encode(65536).unwrap()).outcome,
        DirectoryOutcome::TransferIntentRecorded
    );
    recover_journal(c, false);
    (s, intent)
}
fn publish(
    p: &mut LifecycleDirectory,
    c: &mut LifecycleDirectory,
    s: &DelegationReservationStatus,
    decision: TransferPublicationStatus,
    targets: &mut [Target],
) {
    let operation = s.plan.child_operation().get();
    let completion = DelegationCompletion {
        reservation: s.operation,
        reservation_index: s.index,
        parent_configuration: cfg(),
        child_configuration: cfg(),
        decision: decision.clone(),
    };
    let bytes = completion.encode(100000).unwrap();
    let receipt = command(p, operation + 201, bytes.clone());
    assert!(matches!(
        receipt.outcome,
        DirectoryOutcome::DelegationPublished(_)
    ));
    recover_journal(p, true);
    assert_eq!(command(p, operation + 201, bytes).outcome, receipt.outcome);
    let status = p
        .directory()
        .delegation_publication_at(p.applied_index(), s.operation)
        .unwrap()
        .unwrap();
    assert_eq!(status.completion, completion);
    recover_journal(p, true);
    let activation = TargetActivation {
        metadata_configuration: cfg(),
        decision,
    };
    for t in targets {
        let key = t.application().scope().start() as u8;
        assert_eq!(
            t.read_at(t.applied_index(), query(s.plan.after(), key))
                .unwrap(),
            TargetRead::NotActive
        );
        let bytes = t.activation_command(&activation, 65536).unwrap();
        command(t, operation, bytes);
        recover_target(t, c);
    }
    let parent = current(p, id(500));
    assert_eq!(parent.input().epoch, fixture::parent().input().epoch);
    assert_eq!(parent.input().parent, fixture::parent().input().parent);
    let cache = Cache(BTreeMap::from([
        (id(600), grandparent()),
        (id(500), parent),
        (id(10), current(c, id(10))),
    ]));
    for key in [1, 200] {
        assert_eq!(
            resolve(&cache, &base::Policy, id(600), &[key], 3).unwrap(),
            route(s.plan.after(), key)
        );
    }
}
fn move_again(
    p: &mut LifecycleDirectory,
    c: &mut LifecycleDirectory,
    sources: &mut [Target],
    operation: u128,
    execution: ExecutionMode,
) -> (TransferIntent, Vec<Target>) {
    let (reservation, intent) = reserve(p, c, operation, execution);
    let mut targets = make_targets(&intent, operation);
    for t in &mut targets {
        let bytes = t.bootstrap_command(65536).unwrap();
        command(t, operation, bytes);
        recover_target(t, c);
    }
    let commands: Vec<_> = sources
        .iter()
        .map(|s| {
            let DirectoryRead::Transfer(Some(original)) = c
                .read_at(
                    c.applied_index(),
                    DirectoryQuery::Transfer(s.status().operation),
                )
                .unwrap()
            else {
                panic!("original source intent")
            };
            assert!(s.freeze_command(&original.intent, 65536, 65536).is_err());
            let bytes = s.freeze_command(&intent, 65536, 65536).unwrap();
            assert!(s
                .validate_proposal(op(operation + 1000), &bytes, std::iter::empty())
                .is_err());
            s.validate_proposal(op(operation), &bytes, std::iter::empty())
                .unwrap();
            bytes
        })
        .collect();
    let mut frozen = Vec::new();
    for (s, bytes) in sources.iter_mut().zip(commands) {
        let receipt = command(s, operation, bytes.clone());
        recover_target(s, c);
        assert_eq!(command(s, operation, bytes).outcome, receipt.outcome);
        let status = s.freeze_status().unwrap().unwrap();
        assert_eq!(status.intent, intent);
        let key = s.application().scope().start() as u8;
        assert_eq!(
            s.read_at(s.applied_index(), query(intent.before(), key))
                .unwrap(),
            TargetRead::Rejected(RoutingError::Fenced)
        );
        recover_target(s, c);
        frozen.push(status);
    }
    for t in &mut targets {
        let g = t.status().group;
        let imports = sources
            .iter()
            .zip(&frozen)
            .filter_map(|(s, f)| {
                f.exports
                    .iter()
                    .find(|e| e.target == g)
                    .map(|e| SourceImport {
                        fence: f.fence,
                        configuration: cfg(),
                        image: s.export_target(g, 65536).unwrap(),
                        digest: e.digest,
                    })
            })
            .collect();
        let import = TargetImport::new(op(operation), intent.clone(), g, imports)
            .unwrap_or_else(|e| panic!("{:?}", e.0));
        let bytes = t.import_command(&import, 65536).unwrap();
        command(t, operation, bytes);
        recover_target(t, c);
    }
    let publication = TransferPublication::new(
        op(operation),
        intent.clone(),
        frozen
            .into_iter()
            .map(|f| {
                SourceFenceEvidence::from_status(cfg(), f).unwrap_or_else(|e| panic!("{:?}", e.0))
            })
            .collect(),
        targets
            .iter()
            .map(|t| {
                TargetReadyEvidence::from_status(cfg(), t.status())
                    .unwrap_or_else(|e| panic!("{:?}", e.0))
            })
            .collect(),
    )
    .unwrap_or_else(|e| panic!("{:?}", e.0));
    command(c, operation + 1, publication.encode(65536).unwrap());
    recover_journal(c, false);
    let decision = c
        .directory()
        .transfer_publication_at(c.applied_index(), op(operation))
        .unwrap()
        .unwrap();
    publish(p, c, &reservation, decision, &mut targets);
    (intent, targets)
}

#[test]
fn delegated_split_merge_split_preserves_actual_lineage_and_recovers_later_movement_actions() {
    let mut p = ready_directory(true, 10);
    let mut c = ready_directory(false, 10);
    command(&mut p, 400, plan().encode(65536).unwrap());
    recover_journal(&mut p, true);
    let s = reservation(&p);
    let intent = s.child_intent(cfg()).unwrap();
    let (source, mut split, decision) = completed_child(&intent, &mut c);
    recover_journal(&mut c, false);
    publish(&mut p, &mut c, &s, decision, &mut split);
    write(&mut split[0], intent.after(), 3, 1, 2, 9, false);
    let (merge, mut merged) = move_again(
        &mut p,
        &mut c,
        &mut split,
        202,
        ExecutionMode::Single(group(23)),
    );
    write(&mut merged[0], merge.after(), 1, 1, 7, 7, true);
    write(&mut merged[0], merge.after(), 3, 1, 2, 9, true);
    write(&mut merged[0], merge.after(), 4, 200, 3, 14, false);
    let (final_intent, mut final_targets) = move_again(
        &mut p,
        &mut c,
        &mut merged,
        204,
        ExecutionMode::Partitioned(vec![
            RouteEntry {
                scope: base::range(0, 128),
                target: RouteTarget::Group(group(24)),
            },
            RouteEntry {
                scope: base::range(128, 256),
                target: RouteTarget::Group(group(25)),
            },
        ]),
    );
    write(
        &mut final_targets[0],
        final_intent.after(),
        1,
        1,
        7,
        7,
        true,
    );
    write(
        &mut final_targets[0],
        final_intent.after(),
        3,
        1,
        2,
        9,
        true,
    );
    write(
        &mut final_targets[1],
        final_intent.after(),
        2,
        200,
        11,
        11,
        true,
    );
    write(
        &mut final_targets[1],
        final_intent.after(),
        4,
        200,
        3,
        14,
        true,
    );
    write(
        &mut final_targets[0],
        final_intent.after(),
        5,
        1,
        1,
        10,
        false,
    );
    write(
        &mut final_targets[1],
        final_intent.after(),
        6,
        200,
        2,
        16,
        false,
    );
    for (t, (key, value)) in final_targets.iter_mut().zip([(1, 10), (200, 16)]) {
        recover_target(t, &c);
        assert_eq!(
            t.read_at(t.applied_index(), query(final_intent.after(), key))
                .unwrap(),
            TargetRead::Data(value)
        );
        assert_eq!(t.application().outbox().count(), 3);
    }
    assert_eq!(
        current(&p, id(500)).input().epoch,
        OwnershipEpoch::new(1).unwrap()
    );
    assert_eq!(
        current(&p, id(500)).input().generation,
        RouteGeneration::new(4).unwrap()
    );
    for owner in split.iter().chain(&merged) {
        assert!(owner.fence().is_some());
    }
    assert!(matches!(
        source
            .read_at(
                source.applied_index(),
                voteboat::transfer_source::SourceQuery::Freeze
            )
            .unwrap(),
        voteboat::transfer_source::SourceRead::Freeze(Some(_))
    ));
}
