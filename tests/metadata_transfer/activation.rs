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
fn source() -> MetadataPublishingSource {
    MetadataPublishingSource::new(fresh(16)).unwrap()
}
fn source_apply(
    a: &mut MetadataPublishingSource,
    id: u128,
    bytes: Vec<u8>,
) -> MetadataPublishingOutcome {
    a.apply_batch(&[entry(a.applied_index() + 1, id, bytes)])
        .unwrap()
        .remove(0)
        .outcome
}
pub(super) fn target_apply(
    a: &mut MetadataServingTarget,
    id: u128,
    bytes: Vec<u8>,
) -> MetadataServingOutcome {
    a.apply_batch(&[entry(a.applied_index() + 1, id, bytes)])
        .unwrap()
        .remove(0)
        .outcome
}
fn make_target(plan: MetadataMovePlan) -> MetadataServingTarget {
    MetadataServingTarget::new(
        source(),
        plan,
        op(7),
        ConfigurationId::new(1).unwrap(),
        ConfigurationId::new(2).unwrap(),
    )
    .unwrap()
}
pub(super) fn frozen() -> (MetadataPublishingSource, MetadataImage, Vec<LogEntry>) {
    let mut s = source();
    let boot = s.bootstrap_command(40).unwrap();
    let publish = DirectoryCommand {
        expected: None,
        manifest: grant(),
    }
    .encode(100000)
    .unwrap();
    let mut entries = vec![entry(1, 1000, boot), entry(2, 1001, publish)];
    s.apply_batch(&entries).unwrap();
    let plan = s.source().plan(group(9)).unwrap();
    let mut bad = grant().into_input();
    bad.generation = RouteGeneration::new(2).unwrap();
    let bad = MetadataMovePlan::new(
        group(1),
        group(9),
        vec![ResponsibilityManifest::new(bad).unwrap()],
    )
    .unwrap();
    let bad = s.source().freeze_command(&bad, 100000).unwrap();
    entries.push(entry(3, 6, bad));
    for index in 4..=11 {
        entries.push(LogEntry {
            index,
            term: 1,
            payload: EntryPayload::Noop,
        });
    }
    entries.push(entry(
        12,
        7,
        s.source().freeze_command(&plan, 100000).unwrap(),
    ));
    s.apply_batch(&entries[2..]).unwrap();
    let image = s.source().export(100000).unwrap();
    (s, image, entries)
}
pub(super) fn imported(image: &MetadataImage) -> (MetadataServingTarget, Vec<LogEntry>) {
    let mut t = make_target(image.plan().clone());
    let entries = vec![
        entry(1, 7, t.bootstrap_command(40).unwrap()),
        entry(
            2,
            7,
            t.import_command(image, ConfigurationId::new(1).unwrap(), 100000)
                .unwrap(),
        ),
    ];
    t.apply_batch(&entries).unwrap();
    (t, entries)
}
pub(super) fn publication(
    s: &mut MetadataPublishingSource,
    t: &MetadataServingTarget,
) -> (MetadataPublicationStatus, Vec<u8>) {
    let command = s
        .publication_command(
            t.status().target.imported.unwrap(),
            ConfigurationId::new(2).unwrap(),
            100000,
        )
        .unwrap();
    let MetadataPublishingOutcome::Published(p) = source_apply(s, 7, command.clone()) else {
        panic!("publication")
    };
    (p, command)
}
fn query(t: &MetadataServingTarget) -> MetadataServingRead {
    t.read_at(
        t.applied_index(),
        MetadataServingQuery::Directory(DirectoryQuery::Manifest(grant().input().responsibility)),
    )
    .unwrap()
}
fn updated() -> Vec<u8> {
    let mut m = grant().into_input();
    m.authority = group(9);
    m.generation = RouteGeneration::new(3).unwrap();
    DirectoryCommand {
        expected: Some(RouteGeneration::new(2).unwrap()),
        manifest: ResponsibilityManifest::new(m).unwrap(),
    }
    .encode(100000)
    .unwrap()
}
#[test]
fn publication_activation_and_new_writes_preserve_original_history_and_indices() {
    let (mut s, image, _) = frozen();
    let (mut t, _) = imported(&image);
    assert_eq!(query(&t), MetadataServingRead::NotActive);
    assert_eq!(
        target_apply(&mut t, 90, updated()),
        MetadataServingOutcome::NotActive
    );
    let (p, publish) = publication(&mut s, &t);
    assert_eq!(p.index, 13);
    assert_eq!(p.imported.source.index, 12);
    assert_eq!(p.imported.index, 2);
    let activate = t.activation_command(p, 100000).unwrap();
    assert!(t
        .validate_proposal(op(7), &activate, std::iter::empty())
        .is_ok());
    let MetadataServingOutcome::Activated(a) = target_apply(&mut t, 7, activate.clone()) else {
        panic!("activation")
    };
    assert_eq!(a.index, 4);
    let MetadataServingRead::Directory(DirectoryRead::Manifest(Some(m))) = query(&t) else {
        panic!("manifest")
    };
    assert_eq!(m, image.plan().updated_manifests()[0]);
    assert!(t
        .validate_proposal(op(90), &updated(), std::iter::empty())
        .is_ok());
    assert!(matches!(
        target_apply(&mut t, 90, updated()),
        MetadataServingOutcome::Directory(DirectoryReceipt {
            index: 5,
            outcome: DirectoryOutcome::Published(_),
            duplicate: false,
            ..
        })
    ));
    assert!(matches!(
        target_apply(&mut t, 90, updated()),
        MetadataServingOutcome::Directory(DirectoryReceipt {
            duplicate: true,
            ..
        })
    ));
    let original = DirectoryCommand {
        expected: None,
        manifest: grant(),
    }
    .encode(100000)
    .unwrap();
    assert_eq!(
        target_apply(&mut t, 1001, original),
        MetadataServingOutcome::Historical {
            source: group(1),
            index: 2,
            outcome: DirectoryOutcome::Published(RouteGeneration::new(1).unwrap())
        }
    );
    assert_eq!(
        target_apply(&mut t, 6, updated()),
        MetadataServingOutcome::Conflict
    );
    assert_eq!(
        target_apply(&mut t, 7, updated()),
        MetadataServingOutcome::Conflict
    );
    assert_eq!(
        source_apply(&mut s, 7, publish.clone()),
        MetadataPublishingOutcome::Published(p)
    );
    assert_eq!(
        target_apply(&mut t, 7, activate.clone()),
        MetadataServingOutcome::Activated(a)
    );
    assert_eq!(s.source().export(100000).unwrap(), image);
    assert!(matches!(
        source_apply(&mut s, 91, updated()),
        MetadataPublishingOutcome::Source(MetadataSourceOutcome::Fenced)
    ));
    assert_eq!(s.source().export(100000).unwrap(), image);
    let cp = t
        .checkpoint(t.readiness_requirements().snapshot_bytes)
        .unwrap();
    let mut reopened = make_target(image.plan().clone());
    reopened
        .restore_checkpoint(METADATA_SERVING_SCHEMA, t.applied_index(), &cp)
        .unwrap();
    assert_eq!(query(&reopened), query(&t));
    assert_eq!(reopened.status(), t.status());
    assert_eq!(reopened.imported_image(), Some(&image));
    assert!(matches!(
        target_apply(&mut reopened, 90, updated()),
        MetadataServingOutcome::Directory(DirectoryReceipt {
            duplicate: true,
            ..
        })
    ));
    assert_eq!(
        target_apply(&mut reopened, 7, activate),
        MetadataServingOutcome::Activated(a)
    );
    let cp = s
        .checkpoint(s.readiness_requirements().snapshot_bytes)
        .unwrap();
    let mut reopened = source();
    reopened
        .restore_checkpoint(METADATA_PUBLISHING_SCHEMA, s.applied_index(), &cp)
        .unwrap();
    assert_eq!(reopened.publication(), Some(p));
    assert_eq!(reopened.source().export(100000).unwrap(), image);
    assert_eq!(
        source_apply(&mut reopened, 7, publish),
        MetadataPublishingOutcome::Published(p)
    );
}
#[test]
fn activation_bindings_early_attempts_and_partial_batches_refuse_atomically() {
    let (mut s, image, _) = frozen();
    let (mut t, _) = imported(&image);
    let (p, publish) = publication(&mut s, &t);
    let command = t.activation_command(p, 100000).unwrap();
    assert!(t.activation_command(p, command.len() - 1).is_err());
    for at in [8, 40, 48, 80, 88, 96, 144, 184, 280] {
        let mut changed = command.clone();
        changed[at] ^= 1;
        let before = t.checkpoint(100000).unwrap();
        assert!(
            t.apply_batch(&[entry(3, 7, changed)]).is_err(),
            "offset {at}"
        );
        assert_eq!(t.checkpoint(100000).unwrap(), before);
    }
    for n in 0..command.len() {
        assert!(t
            .validate_proposal(op(7), &command[..n], std::iter::empty())
            .is_err());
    }
    let mut wrong = p;
    wrong.target_configuration = ConfigurationId::new(3).unwrap();
    assert!(t.activation_command(wrong, 100000).is_err());
    wrong = p;
    wrong.imported.source_configuration = ConfigurationId::new(3).unwrap();
    assert!(t.activation_command(wrong, 100000).is_err());
    let mut early = make_target(image.plan().clone());
    assert!(early.apply_batch(&[entry(1, 7, command.clone())]).is_err());
    let before = t.checkpoint(100000).unwrap();
    assert!(t
        .apply_batch(&[entry(3, 7, command.clone()), entry(5, 90, updated())])
        .is_err());
    assert_eq!(t.checkpoint(100000).unwrap(), before);
    assert!(t
        .validate_proposal(
            op(90),
            &updated(),
            [(op(7), command.as_slice())].into_iter()
        )
        .is_ok());
    assert!(matches!(
        target_apply(&mut t, 7, command.clone()),
        MetadataServingOutcome::Activated(_)
    ));
    let mut changed = publish.clone();
    changed[40] ^= 1;
    assert_eq!(
        source_apply(&mut s, 7, changed),
        MetadataPublishingOutcome::Conflict
    );
    let mut changed = command;
    changed[40] ^= 1;
    assert_eq!(
        target_apply(&mut t, 7, changed),
        MetadataServingOutcome::Conflict
    );
    let boot = s.source().bootstrap_command(100000).unwrap();
    assert!(source()
        .validate_proposal(op(1000), &boot, std::iter::empty())
        .is_err());
}
#[test]
fn checkpoints_refuse_truncation_profile_changes_and_replay_history_collisions() {
    let (mut s, image, _) = frozen();
    let (mut t, _) = imported(&image);
    let (p, _) = publication(&mut s, &t);
    let activate = t.activation_command(p, 100000).unwrap();
    target_apply(&mut t, 7, activate);
    target_apply(&mut t, 90, updated());
    let cp = t.checkpoint(100000).unwrap();
    let empty = make_target(image.plan().clone());
    let empty_cp = empty.checkpoint(100000).unwrap();
    for n in 0..cp.len() {
        let mut copy = empty.clone();
        assert!(copy
            .restore_checkpoint(METADATA_SERVING_SCHEMA, t.applied_index(), &cp[..n])
            .is_err());
        assert_eq!(copy.checkpoint(100000).unwrap(), empty_cp);
    }
    let inner_len = u32::from_le_bytes(cp[40..44].try_into().unwrap()) as usize;
    let activation_len_at = 44 + inner_len + 8;
    let activation_len = u32::from_le_bytes(
        cp[activation_len_at..activation_len_at + 4]
            .try_into()
            .unwrap(),
    ) as usize;
    let tail = activation_len_at + 4 + activation_len + 4;
    for operation in [7u128, 6, 1001] {
        let mut changed = cp.clone();
        changed[tail + 8..tail + 24].copy_from_slice(&operation.to_le_bytes());
        assert!(empty
            .clone()
            .restore_checkpoint(METADATA_SERVING_SCHEMA, t.applied_index(), &changed)
            .is_err());
    }
    let wrong = MetadataServingTarget::new(
        source(),
        image.plan().clone(),
        op(7),
        ConfigurationId::new(1).unwrap(),
        ConfigurationId::new(3).unwrap(),
    )
    .unwrap();
    assert!(wrong
        .clone()
        .restore_checkpoint(METADATA_SERVING_SCHEMA, t.applied_index(), &cp)
        .is_err());
    assert!(empty
        .clone()
        .restore_checkpoint(METADATA_TARGET_SCHEMA, t.applied_index(), &cp)
        .is_err());
    let cp = s.checkpoint(100000).unwrap();
    for n in 0..cp.len() {
        assert!(source()
            .restore_checkpoint(METADATA_PUBLISHING_SCHEMA, s.applied_index(), &cp[..n])
            .is_err());
    }
    assert!(source()
        .restore_checkpoint(METADATA_SOURCE_SCHEMA, s.applied_index(), &cp)
        .is_err());
}
fn create_intent(authority: GroupIdentity, id: u128, generation: u64) -> GroupCreationIntent {
    GroupCreationIntent {
        authority,
        parent: grant().input().responsibility,
        expected: RouteGeneration::new(generation).unwrap(),
        responsibility: ResponsibilityIdentity {
            id: ResponsibilityId::new(id).unwrap(),
            incarnation: ResponsibilityIncarnation::new(1).unwrap(),
        },
        bootstrap: support::bootstrap(id, 3),
        application: grant().input().application,
        mode: GroupCreationMode::Empty,
    }
}
fn ready_namespace(
    creation: GroupCreationStatus,
) -> voteboat::namespace_creation::NamespacePublication {
    use voteboat::{namespace_creation::*, routed::*};
    let mut manifest = grant().into_input();
    manifest.authority = creation.intent.authority;
    manifest.responsibility = creation.intent.responsibility;
    manifest.execution = ExecutionMode::Single(creation.intent.bootstrap.group);
    let plan = NamespacePlan {
        creation,
        manifest: ResponsibilityManifest::new(manifest).unwrap(),
    };
    let mut target = CreatedNamespace::new(
        plan,
        Counter::new(8).unwrap(),
        fixture::Policy,
        RoutedLimits {
            operations: 8,
            semantic_bytes: 10000,
            payload_bytes: 1024,
            inner_checkpoint_bytes: 10000,
        },
    )
    .unwrap_or_else(|_| panic!("namespace"));
    let command = target.initialization_command(100000).unwrap();
    let receipt = target
        .apply_batch(&[entry(1, target.plan().creation.operation.get(), command)])
        .unwrap()
        .remove(0);
    assert_eq!(receipt.outcome, NamespaceOutcome::Ready);
    NamespacePublication::from_status(target.plan(), target.status()).unwrap()
}
#[test]
fn inherited_creation_reservations_survive_and_new_namespace_publication_works() {
    let mut s = source();
    let boot = s.bootstrap_command(40).unwrap();
    source_apply(&mut s, 1000, boot);
    source_apply(
        &mut s,
        1001,
        DirectoryCommand {
            expected: None,
            manifest: grant(),
        }
        .encode(100000)
        .unwrap(),
    );
    let old = create_intent(group(1), 30, 1);
    assert!(matches!(
        source_apply(&mut s, 1002, old.encode(100000).unwrap()),
        MetadataPublishingOutcome::Source(MetadataSourceOutcome::Directory(DirectoryReceipt {
            outcome: DirectoryOutcome::CreationReserved,
            ..
        }))
    ));
    let old_status = s
        .source()
        .directory()
        .unwrap()
        .directory()
        .group_creation_at(s.applied_index(), group(30))
        .unwrap()
        .unwrap();
    let old_pub = ready_namespace(old_status.clone());
    source_apply(&mut s, 1003, old_pub.encode(100000).unwrap());
    let plan = s.source().plan(group(9)).unwrap();
    let freeze = s.source().freeze_command(&plan, 100000).unwrap();
    source_apply(&mut s, 7, freeze);
    let image = s.source().export(100000).unwrap();
    let (mut t, _) = imported(&image);
    let (p, _) = publication(&mut s, &t);
    let activate = t.activation_command(p, 100000).unwrap();
    target_apply(&mut t, 7, activate);
    let MetadataServingRead::Creation(Some(inherited)) = t
        .read_at(t.applied_index(), MetadataServingQuery::Creation(group(30)))
        .unwrap()
    else {
        panic!("creation")
    };
    assert_eq!(inherited.authority, group(1));
    assert_eq!(inherited.decode().unwrap(), old_status);
    let mut reused = create_intent(group(9), 30, 2);
    reused.bootstrap.group.incarnation = GroupIncarnation::new(2).unwrap();
    assert!(matches!(
        target_apply(&mut t, 90, reused.encode(100000).unwrap()),
        MetadataServingOutcome::Directory(DirectoryReceipt {
            outcome: DirectoryOutcome::CreationConflict,
            ..
        })
    ));
    let new = create_intent(group(9), 40, 2);
    let receipt = target_apply(&mut t, 91, new.encode(100000).unwrap());
    assert!(matches!(
        receipt,
        MetadataServingOutcome::Directory(DirectoryReceipt {
            outcome: DirectoryOutcome::CreationReserved,
            ..
        })
    ));
    let MetadataServingRead::Creation(Some(created)) = t
        .read_at(t.applied_index(), MetadataServingQuery::Creation(group(40)))
        .unwrap()
    else {
        panic!("creation")
    };
    assert_eq!(created.authority, group(9));
    let new_pub = ready_namespace(created.decode().unwrap());
    assert!(matches!(
        target_apply(&mut t, 92, new_pub.encode(100000).unwrap()),
        MetadataServingOutcome::Directory(DirectoryReceipt {
            outcome: DirectoryOutcome::NamespacePublished(_),
            ..
        })
    ));
    let original = t
        .read_at(
            t.applied_index(),
            MetadataServingQuery::Historical(DirectoryQuery::Manifest(old.responsibility)),
        )
        .unwrap();
    let current = t
        .read_at(
            t.applied_index(),
            MetadataServingQuery::Directory(DirectoryQuery::Manifest(old.responsibility)),
        )
        .unwrap();
    assert!(
        matches!(original, MetadataServingRead::Historical { source: g, value: DirectoryRead::Manifest(Some(_)), .. } if g == group(1))
    );
    assert!(
        matches!(current, MetadataServingRead::Directory(DirectoryRead::Manifest(Some(m))) if m.input().authority == group(9) && m.input().generation.get() == 2)
    );
    let before = t.checkpoint(100000).unwrap();
    let mut reopened = make_target(image.plan().clone());
    reopened
        .restore_checkpoint(METADATA_SERVING_SCHEMA, t.applied_index(), &before)
        .unwrap();
    assert_eq!(reopened.checkpoint(100000).unwrap(), before);
    assert!(matches!(
        target_apply(&mut reopened, 92, new_pub.encode(100000).unwrap()),
        MetadataServingOutcome::Directory(DirectoryReceipt {
            duplicate: true,
            ..
        })
    ));
}

#[cfg(feature = "native")]
#[test]
fn native_publication_and_activation_cuts_preserve_single_authority() {
    use support::{Fault, ModelIo};
    use voteboat::native::log_store::{LogCodec, NativeLogCodec, NativeLogStore};
    fn cuts<A: StateMachine + Clone>(
        group_id: u128,
        entries: Vec<LogEntry>,
        initial: A,
        inspect: impl Fn(&A, bool),
    ) {
        let limits = LogLimits::default();
        let count = entries.len();
        let seed = || {
            let io = ModelIo::default();
            let mut log = NativeLogStore::create(io.clone(), support::identity(1), limits).unwrap();
            support::append(
                &mut log,
                vec![LogMutation::Create(support::bootstrap(group_id, 3))],
            );
            let state = log.state(group(group_id)).unwrap();
            support::append(
                &mut log,
                vec![support::update(
                    &state,
                    1,
                    entries[count - 2].index,
                    Some(Suffix {
                        from: 1,
                        entries: entries[..count - 1].to_vec(),
                    }),
                )],
            );
            (io, log)
        };
        let (_, log) = seed();
        let mutation = support::update(
            &log.state(group(group_id)).unwrap(),
            1,
            entries[count - 1].index,
            Some(Suffix {
                from: entries[count - 1].index,
                entries: entries[count - 1..].to_vec(),
            }),
        );
        let frame = NativeLogCodec
            .encode_batch(3, std::slice::from_ref(&mutation), limits)
            .unwrap();
        let mut outcomes = [false; 2];
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
            let state = log.state(group(group_id)).unwrap();
            let complete = state.commit_index == entries[count - 1].index;
            outcomes[usize::from(complete)] = true;
            let mut recovered = initial.clone();
            recovered
                .apply_batch(
                    &state
                        .entries
                        .into_iter()
                        .filter(|e| e.index <= state.commit_index)
                        .collect::<Vec<_>>(),
                )
                .unwrap();
            inspect(&recovered, complete);
            let mut retried = recovered;
            let mut retry = entries[count - 1].clone();
            retry.index = retried.applied_index() + 1;
            retried.apply_batch(&[retry]).unwrap();
            inspect(&retried, true);
        }
        assert_eq!(outcomes, [true, true]);
    }
    let (mut s, image, mut source_entries) = frozen();
    let (mut t, mut target_entries) = imported(&image);
    let (p, publish) = publication(&mut s, &t);
    source_entries.push(entry(13, 7, publish));
    cuts(1, source_entries, source(), |recovered, complete| {
        assert_eq!(recovered.publication().is_some(), complete);
        assert_eq!(recovered.source().export(100000).unwrap(), image);
        assert!(recovered.source().directory().is_none());
        let cp = recovered.checkpoint(100000).unwrap();
        let mut copy = source();
        copy.restore_checkpoint(METADATA_PUBLISHING_SCHEMA, recovered.applied_index(), &cp)
            .unwrap();
        assert_eq!(copy.publication(), recovered.publication());
    });
    let command = t.activation_command(p, 100000).unwrap();
    target_entries.push(entry(3, 7, command));
    cuts(
        9,
        target_entries.clone(),
        make_target(image.plan().clone()),
        |recovered, complete| {
            assert_eq!(recovered.status().activation.is_some(), complete);
            assert_eq!(
                matches!(query(recovered), MetadataServingRead::Directory(_)),
                complete
            );
            let cp = recovered.checkpoint(100000).unwrap();
            let mut copy = make_target(image.plan().clone());
            copy.restore_checkpoint(METADATA_SERVING_SCHEMA, recovered.applied_index(), &cp)
                .unwrap();
            assert_eq!(query(&copy), query(recovered));
        },
    );
    t.apply_batch(&target_entries[2..]).unwrap();
    target_entries.push(entry(4, 90, updated()));
    cuts(
        9,
        target_entries,
        make_target(image.plan().clone()),
        |recovered, complete| {
            let MetadataServingRead::Directory(DirectoryRead::Manifest(Some(m))) = query(recovered)
            else {
                panic!("active")
            };
            assert_eq!(m.input().generation.get(), if complete { 3 } else { 2 });
        },
    );
}
