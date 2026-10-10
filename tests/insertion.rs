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
#[path = "transfer_source/fixtures.rs"]
mod fixture;
mod support;
use fixture::*;
use voteboat::{
    application::*, bucket_counter::*, directory::*, identity::*, routed::*, routing::*,
    transfer::*, transfer_publication::*, transfer_source::*, transfer_target::*,
};
type Target = TransferTarget<BucketCounter<Policy>, Policy>;
fn commit<A: StateMachine>(a: &mut A, id: u128, bytes: Vec<u8>) -> A::Receipt {
    a.apply_batch(&[entry(a.applied_index() + 1, id, bytes)])
        .unwrap()
        .remove(0)
}
fn directory() -> Directory {
    Directory::new(
        DirectoryPlan::new(group(1), vec![grant()]).unwrap(),
        DirectoryLimits {
            operations: 32,
            history_bytes: 200000,
        },
    )
    .unwrap()
    .with_responsibility_insertion()
    .unwrap_or_else(|_| panic!("schema5"))
}
fn setup() -> (Directory, TransferIntent) {
    setup_in(directory())
}
fn setup_in(mut d: Directory) -> (Directory, TransferIntent) {
    let b = d.bootstrap_command(100000).unwrap();
    commit(&mut d, 100, b);
    commit(
        &mut d,
        101,
        DirectoryCommand {
            expected: None,
            manifest: grant(),
        }
        .encode(100000)
        .unwrap(),
    );
    let mut children = Vec::new();
    for (g, scope) in [(21, range(0, 128)), (22, range(128, 256))] {
        let mut m = grant().into_input();
        m.responsibility.id = ResponsibilityId::new(g).unwrap();
        m.parent = Some(ParentAuthority {
            responsibility: grant().input().responsibility,
            group: group(1),
        });
        m.scope = scope;
        m.execution = ExecutionMode::Single(group(g));
        let m = ResponsibilityManifest::new(m).unwrap();
        let reservation = GroupCreationIntent {
            authority: group(1),
            parent: grant().input().responsibility,
            expected: grant().input().generation,
            responsibility: m.input().responsibility,
            bootstrap: support::bootstrap(g, 3),
            application: m.input().application,
            mode: GroupCreationMode::Staging,
        };
        assert_eq!(
            commit(&mut d, g, reservation.encode(100000).unwrap()).outcome,
            DirectoryOutcome::CreationReserved
        );
        let status = d
            .group_creation_at(d.applied_index(), group(g))
            .unwrap()
            .unwrap();
        children.push(InsertionChild::from_creation(m, &status).unwrap());
    }
    let mut after = grant().into_input();
    after.epoch = OwnershipEpoch::new(2).unwrap();
    after.generation = RouteGeneration::new(2).unwrap();
    after.execution = ExecutionMode::Delegated(
        children
            .iter()
            .map(|c| RouteEntry {
                scope: c.manifest.input().scope,
                target: RouteTarget::Child(ChildAuthority {
                    responsibility: c.manifest.input().responsibility,
                    group: group(1),
                    epoch: c.manifest.input().epoch,
                }),
            })
            .collect(),
    );
    let intent = TransferIntent::insert_children(
        grant(),
        ResponsibilityManifest::new(after).unwrap(),
        children,
    )
    .unwrap();
    (d, intent)
}
fn target(intent: &TransferIntent, g: u128) -> Target {
    Target::new(
        group(g),
        op(200),
        intent.clone(),
        BucketCounter::new(
            intent.target_manifest(group(g)).unwrap().input().scope,
            Policy,
            bucket_limits(),
        )
        .unwrap(),
        Policy,
        TargetLimits {
            import_bytes: 32768,
            application_checkpoint_bytes: bucket_limits().checkpoint_bound().unwrap(),
        },
    )
    .unwrap_or_else(|e| panic!("{:?}", e.0))
}
#[test]
fn insertion_codec_and_reservation_references_are_checked_before_locking() {
    let (mut d, intent) = setup();
    let bytes = intent.encode(100000).unwrap();
    assert_eq!(TransferIntent::decode(&bytes).unwrap(), intent);
    for end in 0..bytes.len() {
        assert!(TransferIntent::decode(&bytes[..end]).is_err());
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(TransferIntent::decode(&trailing).is_err());
    let mut children = intent.insertion_children().unwrap().to_vec();
    children[0].creation_index += 1;
    let wrong =
        TransferIntent::insert_children(intent.before().clone(), intent.after().clone(), children)
            .unwrap();
    assert_eq!(
        commit(&mut d, 199, wrong.encode(100000).unwrap()).outcome,
        DirectoryOutcome::TransferEvidenceMismatch
    );
    assert_eq!(
        commit(&mut d, 200, bytes).outcome,
        DirectoryOutcome::TransferIntentRecorded
    );
    let cp = d.checkpoint(100000).unwrap();
    let mut restored = directory();
    restored
        .restore_checkpoint(5, d.applied_index(), &cp)
        .unwrap();
    assert_eq!(
        restored
            .transfer_intent_at(d.applied_index(), op(200))
            .unwrap()
            .unwrap()
            .intent,
        intent
    );
    let mut legacy = Directory::new(d.plan().clone(), d.limits())
        .unwrap()
        .with_namespace_transfers()
        .unwrap_or_else(|_| panic!("schema4"));
    let boot = legacy.bootstrap_command(100000).unwrap();
    commit(&mut legacy, 100, boot);
    commit(
        &mut legacy,
        101,
        DirectoryCommand {
            expected: None,
            manifest: grant(),
        }
        .encode(100000)
        .unwrap(),
    );
    let old = legacy.checkpoint(100000).unwrap();
    assert!(legacy
        .apply_batch(&[entry(3, 200, intent.encode(100000).unwrap())])
        .is_err());
    assert_eq!(legacy.checkpoint(100000).unwrap(), old);
    for mutation in 0..4 {
        let mut children = intent.insertion_children().unwrap().to_vec();
        let mut m = children[0].manifest.clone().into_input();
        match mutation {
            0 => m.parent = None,
            1 => m.epoch = OwnershipEpoch::new(2).unwrap(),
            2 => m.responsibility = grant().input().responsibility,
            _ => m.execution = ExecutionMode::Single(group(20)),
        }
        let Ok(manifest) = ResponsibilityManifest::new(m) else {
            assert_eq!(mutation, 2);
            continue;
        };
        children[0].manifest = manifest;
        assert!(TransferIntent::insert_children(
            intent.before().clone(),
            intent.after().clone(),
            children
        )
        .is_err());
    }
    assert!(legacy
        .clone()
        .restore_checkpoint(5, d.applied_index(), &cp)
        .is_err());
}
#[test]
fn fenced_parent_imports_into_exact_children_and_publication_recovers_atomically() {
    let (mut d, intent) = setup();
    assert_eq!(
        commit(&mut d, 200, intent.encode(100000).unwrap()).outcome,
        DirectoryOutcome::TransferIntentRecorded
    );
    let mut source = ready();
    commit(&mut source, 1, data(1, 7));
    commit(&mut source, 2, data(200, 11));
    commit(
        &mut source,
        200,
        Source::freeze_command(&intent, 100000).unwrap(),
    );
    let SourceRead::Freeze(Some(status)) = source
        .read_at(source.applied_index(), SourceQuery::Freeze)
        .unwrap()
    else {
        panic!("fence")
    };
    let evidence =
        SourceFenceEvidence::from_status(ConfigurationId::new(1).unwrap(), status.clone())
            .unwrap_or_else(|e| panic!("{:?}", e.0));
    let (mut targets, facts) = import_inserted_targets(&source, &intent, &status);
    let mut wrong = facts.clone();
    wrong[0].configuration = ConfigurationId::new(2).unwrap();
    assert!(
        TransferPublication::new(op(200), intent.clone(), vec![evidence.clone()], wrong).is_err()
    );
    let publication = TransferPublication::new(op(200), intent.clone(), vec![evidence], facts)
        .unwrap_or_else(|e| panic!("{:?}", e.0));
    verify_insertion_publication_reservation(&d, &intent, &publication);
    assert_eq!(
        commit(&mut d, 201, publication.encode(100000).unwrap()).outcome,
        DirectoryOutcome::TransferPublished(RouteGeneration::new(2).unwrap())
    );
    assert_eq!(d.reserved_publication_bytes(), 0);
    let stable = d.checkpoint(200000).unwrap();
    let mut probe = directory();
    for end in 0..stable.len() {
        let before = probe.checkpoint(200000).unwrap();
        assert!(probe
            .restore_checkpoint(5, d.applied_index(), &stable[..end])
            .is_err());
        assert_eq!(probe.checkpoint(200000).unwrap(), before);
    }
    let cp = stable;
    let mut restored = directory();
    restored
        .restore_checkpoint(5, d.applied_index(), &cp)
        .unwrap();
    assert_eq!(
        restored.manifest(grant().input().responsibility),
        Some(intent.after())
    );
    for c in intent.insertion_children().unwrap() {
        assert_eq!(
            restored.manifest(c.manifest.input().responsibility),
            Some(&c.manifest)
        );
    }
    let decision = restored
        .transfer_publication_at(restored.applied_index(), op(200))
        .unwrap()
        .unwrap();
    exercise_inserted_targets(&mut targets, &intent, &decision);
    later_inserted_child_transfer(&mut restored, &mut targets, &intent);
    assert!(matches!(
        commit(&mut source, 4, data(1, 1)).outcome,
        RoutedOutcome::Rejected(RoutingError::Fenced)
    ));
}

#[cfg(feature = "native")]
#[test]
fn insertion_intent_native_torn_frames_recover_exact_reservations_and_lock() {
    use support::{Fault, ModelIo};
    use voteboat::{log::*, native::log_store::*};
    let (d, intent) = setup();
    let mut prefix = vec![
        entry(1, 100, d.bootstrap_command(100000).unwrap()),
        entry(
            2,
            101,
            DirectoryCommand {
                expected: None,
                manifest: grant(),
            }
            .encode(100000)
            .unwrap(),
        ),
    ];
    for (index, g) in [(3, 21), (4, 22)] {
        let status = d
            .group_creation_at(d.applied_index(), group(g))
            .unwrap()
            .unwrap();
        prefix.push(entry(index, g, status.intent.encode(100000).unwrap()));
    }
    let limits = LogLimits::default();
    let seed = || {
        let io = ModelIo::default();
        let mut log = NativeLogStore::create(io.clone(), support::identity(1), limits).unwrap();
        support::append(
            &mut log,
            vec![LogMutation::Create(support::bootstrap(1, 3))],
        );
        let state = log.state(group(1)).unwrap();
        support::append(
            &mut log,
            vec![support::update(
                &state,
                1,
                4,
                Some(Suffix {
                    from: 1,
                    entries: prefix.clone(),
                }),
            )],
        );
        (io, log)
    };
    let (_, log) = seed();
    let state = log.state(group(1)).unwrap();
    let mutation = support::update(
        &state,
        1,
        5,
        Some(Suffix {
            from: 5,
            entries: vec![entry(5, 200, intent.encode(100000).unwrap())],
        }),
    );
    let frame = NativeLogCodec
        .encode_batch(3, std::slice::from_ref(&mutation), limits)
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
        let recovered = NativeLogStore::recover(io, support::identity(1), limits).unwrap();
        let state = recovered.state(group(1)).unwrap();
        let mut app = directory();
        app.apply_batch(
            &state
                .entries
                .into_iter()
                .filter(|e| e.index <= state.commit_index)
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let status = app.transfer_intent_at(state.commit_index, op(200)).unwrap();
        if state.commit_index == 4 {
            old = true;
            assert!(status.is_none());
        } else {
            complete = true;
            assert_eq!(state.commit_index, 5);
            assert_eq!(status.unwrap().intent, intent);
        }
        for c in intent.insertion_children().unwrap() {
            assert!(app.manifest(c.manifest.input().responsibility).is_none());
        }
        assert_eq!(app.manifest(grant().input().responsibility), Some(&grant()));
    }
    assert!(old && complete);
}

#[path = "insertion/cross_authority.rs"]
mod cross_authority;
#[path = "insertion/nested.rs"]
mod nested;

fn import_inserted_targets(
    source: &Source,
    intent: &TransferIntent,
    status: &SourceFreezeStatus,
) -> (Vec<Target>, Vec<TargetReadyEvidence>) {
    let mut targets = Vec::new();
    let mut facts = Vec::new();
    for g in [21, 22] {
        let mut t = target(intent, g);
        let boot = t.bootstrap_command(100000).unwrap();
        commit(&mut t, 200, boot);
        let import = TargetImport::new(
            op(200),
            intent.clone(),
            group(g),
            vec![SourceImport {
                fence: source.fence().unwrap(),
                configuration: ConfigurationId::new(1).unwrap(),
                image: source.export_target(group(g), 65536).unwrap(),
                digest: status
                    .exports
                    .iter()
                    .find(|e| e.target == group(g))
                    .unwrap()
                    .digest,
            }],
        )
        .unwrap_or_else(|e| panic!("{:?}", e.0));
        let bytes = t.import_command(&import, 100000).unwrap();
        commit(&mut t, 200, bytes);
        facts.push(
            TargetReadyEvidence::from_status(ConfigurationId::new(1).unwrap(), t.status())
                .unwrap_or_else(|e| panic!("{:?}", e.0)),
        );
        targets.push(t);
    }
    (targets, facts)
}

fn verify_insertion_publication_reservation(
    d: &Directory,
    intent: &TransferIntent,
    publication: &TransferPublication,
) {
    assert!(d
        .manifest(
            intent.insertion_children().unwrap()[0]
                .manifest
                .input()
                .responsibility
        )
        .is_none());
    let old = d.checkpoint(200000).unwrap();
    let mut atomic = d.clone();
    assert_eq!(
        atomic.apply_batch(&[
            entry(
                d.applied_index() + 1,
                201,
                publication.encode(100000).unwrap()
            ),
            noop(d.applied_index() + 3)
        ]),
        Err(ApplicationError::IndexGap)
    );
    assert_eq!(atomic.checkpoint(200000).unwrap(), old);
    let mut exhausted = d.clone();
    let mut fill = 1000;
    while exhausted.remaining_operations() > 0 {
        assert_eq!(
            commit(
                &mut exhausted,
                fill,
                DirectoryCommand {
                    expected: None,
                    manifest: grant()
                }
                .encode(100000)
                .unwrap()
            )
            .outcome,
            DirectoryOutcome::LifecycleBusy
        );
        fill += 1;
    }
    assert!(exhausted
        .validate_proposal(
            op(201),
            &publication.encode(100000).unwrap(),
            std::iter::empty()
        )
        .is_ok());
    assert_eq!(
        commit(&mut exhausted, 201, publication.encode(100000).unwrap()).outcome,
        DirectoryOutcome::TransferPublished(RouteGeneration::new(2).unwrap())
    );
    assert_eq!(exhausted.reserved_publication_bytes(), 0);
}

fn exercise_inserted_targets(
    targets: &mut [Target],
    intent: &TransferIntent,
    decision: &TransferPublicationStatus,
) {
    for (i, t) in targets.iter_mut().enumerate() {
        let activation = TargetActivation {
            metadata_configuration: ConfigurationId::new(1).unwrap(),
            decision: decision.clone(),
        };
        let bytes = t.activation_command(&activation, 100000).unwrap();
        assert!(matches!(
            commit(t, 200, bytes).outcome,
            TargetOutcome::Activated(_)
        ));
        assert_eq!(
            commit(t, 90, data(if i == 0 { 1 } else { 200 }, 1)).outcome,
            TargetOutcome::Rejected(RoutingError::WrongIdentity)
        );
        let child = &intent.insertion_children().unwrap()[i].manifest;
        let key = if i == 0 { 1 } else { 200 };
        let mut h = hint(key);
        h.responsibility = child.input().responsibility;
        h.group = group(21 + i as u128);
        h.scope = child.input().scope;
        assert_eq!(
            t.read_at(
                t.applied_index(),
                TargetQuery::Data(RoutedQuery {
                    hint: h,
                    key: vec![key],
                    query: vec![key]
                })
            )
            .unwrap(),
            TargetRead::Data(if i == 0 { 7 } else { 11 })
        );
        let retry = encode_routed(
            h,
            &[key],
            &encode_add(&[key], if i == 0 { 7 } else { 11 }, b"effect", 1024).unwrap(),
            4096,
        )
        .unwrap();
        assert!(matches!(
            commit(t, if i == 0 { 1 } else { 2 }, retry).outcome,
            TargetOutcome::Applied(_)
        ));
        assert_eq!(
            t.application().value(&[key]),
            Ok(if i == 0 { 7 } else { 11 })
        );
        assert_eq!(t.application().outbox().count(), 1);
        let cmd = encode_routed(
            h,
            &[key],
            &encode_add(&[key], 1, b"effect", 1024).unwrap(),
            4096,
        )
        .unwrap();
        assert!(matches!(
            commit(t, 3, cmd).outcome,
            TargetOutcome::Applied(_)
        ));
        let cp = t.checkpoint(100000).unwrap();
        let mut fresh = target(intent, 21 + i as u128);
        fresh.restore_checkpoint(3, t.applied_index(), &cp).unwrap();
        assert!(fresh.restore_checkpoint(2, t.applied_index(), &cp).is_err());
    }
}

fn later_inserted_child_transfer(
    restored: &mut Directory,
    targets: &mut [Target],
    intent: &TransferIntent,
) {
    let child = intent.insertion_children().unwrap()[0].manifest.clone();
    let mut next = child.clone().into_input();
    next.epoch = OwnershipEpoch::new(2).unwrap();
    next.generation = RouteGeneration::new(2).unwrap();
    next.execution = ExecutionMode::Partitioned(vec![
        RouteEntry {
            scope: range(0, 64),
            target: RouteTarget::Group(group(31)),
        },
        RouteEntry {
            scope: range(64, 128),
            target: RouteTarget::Group(group(32)),
        },
    ]);
    let plan = voteboat::delegation::DelegationPlan::new(
        intent.after().clone(),
        child.clone(),
        ResponsibilityManifest::new(next).unwrap(),
        op(300),
    )
    .unwrap();
    assert_eq!(
        commit(restored, 301, plan.encode(100000).unwrap()).outcome,
        DirectoryOutcome::DelegationReserved
    );
    let next = restored
        .delegation_reservation_at(restored.applied_index(), op(301))
        .unwrap()
        .unwrap()
        .child_intent(ConfigurationId::new(1).unwrap())
        .unwrap();
    assert_eq!(
        commit(restored, 300, next.encode(100000).unwrap()).outcome,
        DirectoryOutcome::TransferIntentRecorded
    );
    let bytes = targets[0].freeze_command(&next, 65536, 100000).unwrap();
    let TargetOutcome::Frozen(fence) = commit(&mut targets[0], 300, bytes).outcome else {
        panic!("later child freeze")
    };
    assert_eq!(fence.responsibility, child.input().responsibility);
    let cp = targets[0].checkpoint(100000).unwrap();
    let mut reopened = target(intent, 21);
    reopened
        .restore_checkpoint(3, targets[0].applied_index(), &cp)
        .unwrap();
}
