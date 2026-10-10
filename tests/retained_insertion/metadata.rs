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
use voteboat::{log::*, metadata_transfer::*};

fn selected(i: &TransferIntent, limit: usize) -> Source {
    retained_source(i)
        .with_metadata_authority_adoption(limit)
        .unwrap_or_else(|_| panic!("metadata profile"))
}
fn frozen(owner: &Source, id: u128) -> ScopedExportStatus {
    let ScopedSourceRead::Frozen(Some(status)) = owner
        .read_at(owner.applied_index(), ScopedSourceQuery::Frozen(op(id)))
        .unwrap()
    else {
        panic!("frozen")
    };
    status
}
fn metadata_source(remaining: bool) -> MetadataPublishingSource {
    let directory = if remaining {
        Directory::new(
            DirectoryPlan::new(group(1), vec![before(false)]).unwrap(),
            DirectoryLimits {
                operations: 32,
                history_bytes: 200000,
            },
        )
        .unwrap()
        .with_remaining_transfer()
        .unwrap_or_else(|_| panic!("remaining profile"))
    } else {
        fresh(before(false), true)
    };
    let d = LifecycleDirectory::new(directory);
    let limit = d.directory().readiness_requirements().snapshot_bytes;
    MetadataPublishingSource::new(
        MetadataAuthoritySource::new(d, limit).unwrap_or_else(|_| panic!("source")),
    )
    .unwrap()
}
fn live(d: &MetadataPublishingSource) -> &Directory {
    d.source().directory().unwrap().directory()
}
struct Moved {
    owner: Source,
    first: TransferIntent,
    retained: RetainedGrantAdoption,
    adoption: OwnerMetadataAdoption,
    target: MetadataServingTarget,
    owner_log: Vec<LogEntry>,
    child: Target,
    child_log: Vec<LogEntry>,
}
fn moved() -> Moved {
    moved_with(None)
}
fn moved_with(profile: Option<bool>) -> Moved {
    // Replay the actual initial reservation into the publishing profile, then
    // perform the transfer under that profile rather than fabricating statuses.
    let (seed, _, first) = setup(false);
    let creation = seed
        .group_creation_at(seed.applied_index(), group(21))
        .unwrap()
        .unwrap();
    let mut d = metadata_source(profile.is_some());
    let boot = d.bootstrap_command(100000).unwrap();
    commit(&mut d, 1000, boot);
    commit(
        &mut d,
        1001,
        DirectoryCommand {
            expected: None,
            manifest: first.before().clone(),
        }
        .encode(100000)
        .unwrap(),
    );
    commit(&mut d, 21, creation.intent.encode(100000).unwrap());
    commit(&mut d, 200, first.encode(100000).unwrap());
    let mut owner = selected(&first, 2);
    let owner_log = vec![
        entry(1, 100, owner.bootstrap_command(100000).unwrap()),
        entry(2, 1, data_at(&first, 1, 7)),
        entry(3, 2, data_at(&first, 200, 11)),
        entry(4, 200, first.encode(100000).unwrap()),
    ];
    owner.apply_batch(&owner_log).unwrap();
    let status = frozen(&owner, 200);
    let mut child = if let Some(partial) = profile {
        imported::selected(&first, partial)
    } else {
        target(&first)
    };
    let mut child_log = Vec::new();
    let boot = child.bootstrap_command(100000).unwrap();
    record_target(&mut child, &mut child_log, 200, boot);
    let import = TargetImport::new(
        op(200),
        first.clone(),
        group(21),
        vec![SourceImport {
            fence: status.fence.fence,
            configuration: ConfigurationId::new(1).unwrap(),
            image: owner.export(op(200), 65536).unwrap(),
            digest: status.digest,
        }],
    )
    .unwrap();
    let command = child.import_command(&import, 100000).unwrap();
    record_target(&mut child, &mut child_log, 200, command);
    let publication = TransferPublication::new(
        op(200),
        first.clone(),
        vec![SourceFenceEvidence::from_scoped_status(
            ConfigurationId::new(1).unwrap(),
            status,
            &first,
        )
        .unwrap()],
        vec![
            TargetReadyEvidence::from_status(ConfigurationId::new(1).unwrap(), child.status())
                .unwrap(),
        ],
    )
    .unwrap();
    commit(&mut d, 201, publication.encode(100000).unwrap());
    let retained = RetainedGrantAdoption {
        metadata_configuration: ConfigurationId::new(1).unwrap(),
        decision: live(&d)
            .transfer_publication_at(d.applied_index(), op(200))
            .unwrap()
            .unwrap(),
    };
    let command = retained.encode(100000).unwrap();
    let mut owner_log = owner_log;
    owner_log.push(entry(5, 300, command));
    owner.apply_batch(&owner_log[4..]).unwrap();
    let activation = child
        .activation_command(
            &TargetActivation {
                metadata_configuration: ConfigurationId::new(1).unwrap(),
                decision: retained.decision.clone(),
            },
            100000,
        )
        .unwrap();
    record_target(&mut child, &mut child_log, 200, activation);
    let (adoption, target) = move_metadata_authority(d, &first, profile);
    Moved {
        owner,
        first,
        retained,
        adoption,
        target,
        owner_log,
        child,
        child_log,
    }
}
fn reopen(owner: &Source, first: &TransferIntent) -> Source {
    let mut copy = selected(first, 2);
    copy.restore_checkpoint(
        owner.schema_version(),
        owner.applied_index(),
        &owner.checkpoint(1000000).unwrap(),
    )
    .unwrap();
    copy
}
fn write(owner: &Source, key: u8, delta: i64) -> Vec<u8> {
    let input = owner.grant().input();
    let hint = RouteHint {
        responsibility: input.responsibility,
        group: group(20),
        application: input.application,
        scheme: input.scheme,
        scope: range(128, 256),
        bucket: key as u16,
        epoch: input.epoch,
        generation: input.generation,
    };
    encode_routed(
        hint,
        &[key],
        &encode_add(&[key], delta, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap()
}
#[test]
fn retained_owner_metadata_move_keeps_exports_and_supports_later_retained_transfer() {
    let Moved {
        mut owner,
        first,
        retained,
        adoption,
        mut target,
        ..
    } = moved();
    let image = owner.export(op(200), 65536).unwrap();
    let bootstrap = owner.bootstrap_command(100000).unwrap();
    let command = adoption.encode(MAX_METADATA_ADOPTION_BYTES).unwrap();
    let receipt = commit(&mut owner, 701, command.clone()).outcome;
    assert_eq!(
        owner.schema_version(),
        METADATA_SCOPED_TRANSFER_SOURCE_SCHEMA
    );
    assert_eq!(owner.grant(), &adoption.after());
    owner = reopen(&owner, &first);
    assert_eq!(owner.bootstrap_command(100000).unwrap(), bootstrap);
    assert_eq!(owner.export(op(200), 65536).unwrap(), image);
    assert_eq!(commit(&mut owner, 701, command.clone()).outcome, receipt);
    let q = ScopedSourceQuery::MetadataAdoption(op(701));
    let read = owner.read_at(owner.applied_index(), q.clone()).unwrap();
    let ScopedSourceRead::MetadataAdoption(Some(status)) = &read else {
        panic!("metadata status")
    };
    assert_eq!(status.activation, adoption.activation());
    assert_eq!(status.owner.metadata_index, adoption.activation().index);
    assert_eq!(owner.read_result_bytes(&read, 0).unwrap(), 0);
    assert_eq!(
        owner.read_result_bound(&q).unwrap(),
        std::mem::size_of::<ScopedSourceRead<i64>>()
    );
    assert!(matches!(
        commit(&mut owner, 300, retained.encode(100000).unwrap()).outcome,
        RoutedOutcome::GrantAdopted(_)
    ));
    let retry = write(&owner, 200, 11);
    assert!(
        matches!(commit(&mut owner, 2, retry).outcome, RoutedOutcome::Applied(r) if r.duplicate)
    );
    let bytes = write(&owner, 150, 5);
    commit(&mut owner, 6, bytes);
    let intent = reserve_metadata_child(&mut target, &owner);
    commit(&mut owner, 202, intent.encode(100000).unwrap());
    let (mut child, decision) = publish_metadata_child(&mut target, &owner, &intent);
    let later = RetainedGrantAdoption {
        metadata_configuration: ConfigurationId::new(2).unwrap(),
        decision: decision.clone(),
    };
    commit(&mut owner, 301, later.encode(100000).unwrap());
    let bytes = child
        .activation_command(
            &TargetActivation {
                metadata_configuration: ConfigurationId::new(2).unwrap(),
                decision,
            },
            100000,
        )
        .unwrap();
    commit(&mut child, 202, bytes);
    check_metadata_child(&mut child);
    assert_eq!(owner.grant(), intent.after());
    owner = reopen(&owner, &first);
    assert_eq!(owner.grant(), intent.after());
    assert_eq!(owner.export(op(200), 65536).unwrap(), image);
    assert_eq!(commit(&mut owner, 701, command).outcome, receipt);
    assert_eq!(owner.routed().application().outbox().count(), 3);
}

#[test]
fn metadata_adoption_profiles_pending_work_and_checkpoint_failures_are_atomic() {
    let Moved {
        mut owner,
        first,
        adoption,
        owner_log,
        ..
    } = moved();
    let bytes = adoption.encode(MAX_METADATA_ADOPTION_BYTES).unwrap();
    check_metadata_profiles(&first, &bytes);
    let before = owner.checkpoint(1000000).unwrap();
    let index = owner.applied_index();
    assert!(owner
        .apply_batch(&[
            entry(index + 1, 701, bytes.clone()),
            entry(index + 2, 702, vec![0])
        ])
        .is_err());
    assert_eq!(owner.checkpoint(1000000).unwrap(), before);
    assert!(owner
        .validate_proposal(op(701), &bytes, [(op(701), bytes.as_slice())].into_iter())
        .is_ok());
    let mut pending = owner.clone();
    commit(&mut pending, 701, bytes.clone());
    let data = write(&pending, 200, 3);
    assert!(owner
        .validate_proposal(op(6), &data, [(op(701), bytes.as_slice())].into_iter())
        .is_ok());
    let receipt = commit(&mut owner, 701, bytes.clone()).outcome;
    let checkpoint = owner.checkpoint(1000000).unwrap();
    assert!(owner
        .apply_batch(&[entry(owner.applied_index() + 1, 702, bytes.clone())])
        .is_err());
    assert_eq!(owner.checkpoint(1000000).unwrap(), checkpoint);
    let mut altered = adoption.activation();
    altered.publication.target_configuration = ConfigurationId::new(3).unwrap();
    let altered = OwnerMetadataAdoption::new(
        adoption.plan().clone(),
        adoption.before().input().responsibility,
        altered,
    )
    .unwrap()
    .encode(MAX_METADATA_ADOPTION_BYTES)
    .unwrap();
    assert!(owner
        .apply_batch(&[entry(owner.applied_index() + 1, 701, altered)])
        .is_err());
    assert_eq!(owner.checkpoint(1000000).unwrap(), checkpoint);
    check_metadata_checkpoints(&first, &owner, &checkpoint);
    owner = reopen(&owner, &first);
    let epoch = owner.grant().input().epoch;
    commit(&mut owner, 900, encode_fence(epoch));
    assert_eq!(commit(&mut owner, 701, bytes.clone()).outcome, receipt);
    let mut fresh = selected(&first, 2);
    assert!(fresh.apply_batch(&[entry(1, 701, bytes.clone())]).is_err());
    fresh.apply_batch(&owner_log).unwrap();
    commit(&mut fresh, 900, encode_fence(epoch));
    assert!(fresh
        .apply_batch(&[entry(fresh.applied_index() + 1, 701, bytes)])
        .is_err());
    owner = reopen(&owner, &first);
    assert_eq!(
        owner.metadata_adoption(op(701)).unwrap().activation,
        adoption.activation()
    );
    assert!(matches!(
        owner
            .read_at(
                owner.applied_index(),
                ScopedSourceQuery::Data(RoutedQuery {
                    hint: source_hint(&first, 200),
                    key: vec![200],
                    query: vec![200]
                })
            )
            .unwrap(),
        ScopedSourceRead::Data(RoutedRead::Rejected(_))
    ));
}

#[test]
#[cfg(feature = "native")]
fn retained_metadata_adoption_native_cuts_keep_original_exports_and_resume() {
    use support::{Fault, ModelIo};
    use voteboat::native::log_store::*;
    let Moved {
        owner,
        first,
        adoption,
        owner_log,
        ..
    } = moved();
    let image = owner.export(op(200), 65536).unwrap();
    let bytes = adoption.encode(MAX_METADATA_ADOPTION_BYTES).unwrap();
    let limits = LogLimits::default();
    let seed = || {
        let io = ModelIo::default();
        let mut store = NativeLogStore::create(io.clone(), support::identity(1), limits).unwrap();
        support::append(
            &mut store,
            vec![LogMutation::Create(support::bootstrap(20, 3))],
        );
        let state = store.state(group(20)).unwrap();
        support::append(
            &mut store,
            vec![support::update(
                &state,
                1,
                5,
                Some(Suffix {
                    from: 1,
                    entries: owner_log.clone(),
                }),
            )],
        );
        (io, store)
    };
    let (_, store) = seed();
    let mutation = support::update(
        &store.state(group(20)).unwrap(),
        1,
        6,
        Some(Suffix {
            from: 6,
            entries: vec![entry(6, 701, bytes.clone())],
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
        let (io, mut store) = seed();
        io.0.borrow_mut().fault = fault;
        if let Ok(tickets) = store.append_batch(vec![mutation.clone()]) {
            assert!(store.barrier(&tickets).is_err());
        }
        drop(store);
        io.0.borrow_mut().power_loss();
        let store = NativeLogStore::recover(io, support::identity(1), limits).unwrap();
        let state = store.state(group(20)).unwrap();
        let mut recovered = selected(&first, 2);
        recovered
            .apply_batch(
                &state
                    .entries
                    .iter()
                    .filter(|e| e.index <= state.commit_index)
                    .cloned()
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        match state.commit_index {
            5 => {
                old = true;
                assert_eq!(recovered.grant(), first.after());
                assert_eq!(recovered.metadata_adoption(op(701)), None);
            }
            6 => {
                complete = true;
                assert_eq!(recovered.grant(), &adoption.after());
                assert_eq!(recovered.metadata_adoption(op(701)).unwrap().owner.index, 6);
            }
            _ => panic!("partial committed adoption"),
        }
        assert_eq!(recovered.export(op(200), 65536).unwrap(), image);
        assert_eq!(recovered.routed().application().outbox().count(), 2);
        let receipt = commit(&mut recovered, 701, bytes.clone()).outcome;
        recovered = reopen(&recovered, &first);
        assert_eq!(commit(&mut recovered, 701, bytes.clone()).outcome, receipt);
        let retry = write(&recovered, 200, 11);
        assert!(
            matches!(commit(&mut recovered,2,retry).outcome,RoutedOutcome::Applied(r) if r.duplicate)
        );
        assert_eq!(recovered.export(op(200), 65536).unwrap(), image);
    }
    assert!(old && complete);
}

fn record_target(
    t: &mut Target,
    log: &mut Vec<LogEntry>,
    id: u128,
    bytes: Vec<u8>,
) -> TargetOutcome<BucketReceipt> {
    let e = entry(t.applied_index() + 1, id, bytes);
    let r = t
        .apply_batch(std::slice::from_ref(&e))
        .unwrap()
        .remove(0)
        .outcome;
    log.push(e);
    r
}
#[path = "metadata/imported.rs"]
mod imported;

fn move_metadata_authority(
    mut d: MetadataPublishingSource,
    first: &TransferIntent,
    profile: Option<bool>,
) -> (OwnerMetadataAdoption, MetadataServingTarget) {
    let plan = d.source().plan(group(9)).unwrap();
    let command = d.source().freeze_command(&plan, 100000).unwrap();
    commit(&mut d, 700, command);
    let image = d.source().export(1000000).unwrap();
    let mut target = MetadataServingTarget::new(
        metadata_source(profile.is_some()),
        plan.clone(),
        op(700),
        ConfigurationId::new(1).unwrap(),
        ConfigurationId::new(2).unwrap(),
    )
    .unwrap();
    let command = target.bootstrap_command(100000).unwrap();
    commit(&mut target, 700, command);
    let command = target
        .import_command(&image, ConfigurationId::new(1).unwrap(), 1000000)
        .unwrap();
    commit(&mut target, 700, command);
    let command = d
        .publication_command(
            target.status().target.imported.unwrap(),
            ConfigurationId::new(2).unwrap(),
            100000,
        )
        .unwrap();
    commit(&mut d, 700, command);
    let command = target
        .activation_command(d.publication().unwrap(), 100000)
        .unwrap();
    commit(&mut target, 700, command);
    assert!(d.source().directory().is_none());
    let adoption = OwnerMetadataAdoption::new(
        plan,
        first.before().input().responsibility,
        target.status().activation.unwrap(),
    )
    .unwrap();

    (adoption, target)
}

fn reserve_metadata_child(target: &mut MetadataServingTarget, owner: &Source) -> TransferIntent {
    let current = owner.grant().clone();
    let mut child = current.clone().into_input();
    child.responsibility = rid(22);
    child.parent = Some(ParentAuthority {
        responsibility: current.input().responsibility,
        group: group(9),
    });
    child.scope = range(128, 192);
    child.epoch = OwnershipEpoch::new(1).unwrap();
    child.generation = RouteGeneration::new(1).unwrap();
    child.execution = ExecutionMode::Single(group(22));
    let child = ResponsibilityManifest::new(child).unwrap();
    let creation = GroupCreationIntent {
        authority: group(9),
        parent: current.input().responsibility,
        expected: current.input().generation,
        responsibility: rid(22),
        bootstrap: support::bootstrap(22, 3),
        application: current.input().application,
        mode: GroupCreationMode::Staging,
    };
    commit(target, 22, creation.encode(100000).unwrap());
    let MetadataServingRead::Creation(Some(created)) = target
        .read_at(
            target.applied_index(),
            MetadataServingQuery::Creation(group(22)),
        )
        .unwrap()
    else {
        panic!("creation")
    };
    let created = GroupCreationStatus {
        operation: created.operation,
        index: created.index,
        intent: GroupCreationIntent::decode(&created.intent).unwrap(),
    };
    let mut after = current.clone().into_input();
    after.epoch = OwnershipEpoch::new(current.input().epoch.get() + 1).unwrap();
    after.generation = RouteGeneration::new(current.input().generation.get() + 1).unwrap();
    let ExecutionMode::Delegated(ref mut routes) = after.execution else {
        panic!("delegated")
    };
    routes[1] = RouteEntry {
        scope: range(128, 192),
        target: RouteTarget::Child(ChildAuthority {
            responsibility: rid(22),
            group: group(9),
            epoch: child.input().epoch,
        }),
    };
    routes.push(RouteEntry {
        scope: range(192, 256),
        target: RouteTarget::Group(group(20)),
    });
    let intent = TransferIntent::insert_retained_child(
        current,
        ResponsibilityManifest::new(after).unwrap(),
        InsertionChild::from_creation(child, &created).unwrap(),
    )
    .unwrap();
    commit(target, 202, intent.encode(100000).unwrap());

    intent
}

fn publish_metadata_child(
    target: &mut MetadataServingTarget,
    owner: &Source,
    intent: &TransferIntent,
) -> (Target, TransferPublicationStatus) {
    let status = frozen(owner, 202);
    let mut child = Target::new(
        group(22),
        op(202),
        intent.clone(),
        BucketCounter::new(range(128, 192), Policy, bucket_limits()).unwrap(),
        Policy,
        TargetLimits {
            import_bytes: 32768,
            application_checkpoint_bytes: bucket_limits().checkpoint_bound().unwrap(),
        },
    )
    .unwrap_or_else(|_| panic!("child"));
    let bytes = child.bootstrap_command(100000).unwrap();
    commit(&mut child, 202, bytes);
    let import = TargetImport::new(
        op(202),
        intent.clone(),
        group(22),
        vec![SourceImport {
            fence: status.fence.fence,
            configuration: ConfigurationId::new(1).unwrap(),
            image: owner.export(op(202), 65536).unwrap(),
            digest: status.digest,
        }],
    )
    .unwrap();
    let bytes = child.import_command(&import, 100000).unwrap();
    commit(&mut child, 202, bytes);
    let publication = TransferPublication::new(
        op(202),
        intent.clone(),
        vec![SourceFenceEvidence::from_scoped_status(
            ConfigurationId::new(1).unwrap(),
            status,
            intent,
        )
        .unwrap()],
        vec![
            TargetReadyEvidence::from_status(ConfigurationId::new(1).unwrap(), child.status())
                .unwrap(),
        ],
    )
    .unwrap();
    commit(target, 203, publication.encode(100000).unwrap());
    let MetadataServingRead::Directory(DirectoryRead::Publication(Some(decision))) = target
        .read_at(
            target.applied_index(),
            MetadataServingQuery::Directory(DirectoryQuery::Publication(op(202))),
        )
        .unwrap()
    else {
        panic!("publication")
    };

    (child, decision)
}

fn check_metadata_profiles(first: &TransferIntent, bytes: &[u8]) {
    for limit in [0, MAX_PARENT_ADOPTIONS] {
        let old = retained_source(first);
        let boot = old.bootstrap_command(100000).unwrap();
        let (err, returned) = old.with_metadata_authority_adoption(limit).err().unwrap();
        assert_eq!(err, ApplicationError::InvalidCommand);
        assert_eq!(returned.bootstrap_command(100000).unwrap(), boot);
    }
    for profile in 0..3 {
        let mut old = match profile {
            0 => retained_source(first),
            1 => retained_source(first)
                .with_parent_adoption(2)
                .unwrap_or_else(|_| panic!("parent")),
            _ => retained_source(first)
                .with_parent_slot_adoption(2)
                .unwrap_or_else(|_| panic!("slots")),
        };
        let boot = old.bootstrap_command(100000).unwrap();
        commit(&mut old, 100, boot);
        let checkpoint = old.checkpoint(1000000).unwrap();
        assert!(old.apply_batch(&[entry(2, 701, bytes.to_vec())]).is_err());
        assert_eq!(old.checkpoint(1000000).unwrap(), checkpoint);
    }
}

fn check_metadata_checkpoints(first: &TransferIntent, owner: &Source, checkpoint: &[u8]) {
    let mut restore = selected(first, 2);
    let clean = restore.checkpoint(1000000).unwrap();
    for end in (0..checkpoint.len())
        .step_by(19)
        .chain(std::iter::once(checkpoint.len() - 1))
    {
        assert!(restore
            .restore_checkpoint(
                METADATA_SCOPED_TRANSFER_SOURCE_SCHEMA,
                owner.applied_index(),
                &checkpoint[..end]
            )
            .is_err());
        assert_eq!(restore.checkpoint(1000000).unwrap(), clean);
    }
    for profile in 0..3 {
        let mut old = match profile {
            0 => retained_source(first),
            1 => retained_source(first)
                .with_parent_adoption(2)
                .unwrap_or_else(|_| panic!("parent")),
            _ => retained_source(first)
                .with_parent_slot_adoption(2)
                .unwrap_or_else(|_| panic!("slots")),
        };
        assert!(old
            .restore_checkpoint(
                METADATA_SCOPED_TRANSFER_SOURCE_SCHEMA,
                owner.applied_index(),
                checkpoint
            )
            .is_err());
    }
}

fn check_metadata_child(child: &mut Target) {
    let m = child.grant().input();
    let hint = RouteHint {
        responsibility: m.responsibility,
        group: group(22),
        application: m.application,
        scheme: m.scheme,
        scope: m.scope,
        bucket: 150,
        epoch: m.epoch,
        generation: m.generation,
    };
    assert_eq!(
        child
            .read_at(
                child.applied_index(),
                TargetQuery::Data(RoutedQuery {
                    hint,
                    key: vec![150],
                    query: vec![150],
                })
            )
            .unwrap(),
        TargetRead::Data(5)
    );
    let retry = encode_routed(
        hint,
        &[150],
        &encode_add(&[150], 5, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap();
    assert!(matches!(commit(child, 6, retry).outcome, TargetOutcome::Applied(r) if r.duplicate));
}
