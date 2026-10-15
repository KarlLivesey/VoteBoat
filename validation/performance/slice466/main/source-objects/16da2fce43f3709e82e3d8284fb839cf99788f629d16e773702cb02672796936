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
use voteboat::{bucket_counter::*, routed::*, transfer_source::*, transfer_target::*};
type Owner = RoutedApplication<BucketCounter<fixture::Policy>, fixture::Policy>;
type Source = TransferSource<BucketCounter<fixture::Policy>, fixture::Policy>;
fn selected(limit: usize) -> Owner {
    fixture::routed()
        .with_metadata_authority_adoption(limit)
        .unwrap_or_else(|_| panic!("profile"))
}
fn source(limit: usize) -> Source {
    TransferSource::new(selected(limit), 65536).unwrap_or_else(|_| panic!("source"))
}
fn moved() -> (OwnerMetadataAdoption, MetadataServingTarget) {
    let (mut s, image, _) = activation::frozen();
    let (mut t, _) = activation::imported(&image);
    let (p, _) = activation::publication(&mut s, &t);
    let command = t.activation_command(p, 100000).unwrap();
    activation::target_apply(&mut t, 7, command);
    let a = OwnerMetadataAdoption::new(
        image.plan().clone(),
        grant().input().responsibility,
        t.status().activation.unwrap(),
    )
    .unwrap();
    (a, t)
}
fn apply<A: StateMachine>(a: &mut A, id: u128, bytes: Vec<u8>) -> A::Receipt {
    a.apply_batch(&[entry(a.applied_index() + 1, id, bytes)])
        .unwrap()
        .remove(0)
}
fn data(m: &ResponsibilityManifest, key: u8, delta: i64) -> Vec<u8> {
    let input = m.input();
    let hint = RouteHint {
        responsibility: input.responsibility,
        group: group(20),
        application: input.application,
        scheme: input.scheme,
        scope: input.scope,
        bucket: key as u16,
        epoch: input.epoch,
        generation: input.generation,
    };
    encode_routed(
        hint,
        &[key],
        &encode_add(&[key], delta, b"outbox", 1024).unwrap(),
        4096,
    )
    .unwrap()
}
#[test]
fn full_owner_adopts_activated_metadata_with_original_data_and_control_retries() {
    let (a, _) = moved();
    let command = a.encode(MAX_METADATA_ADOPTION_BYTES).unwrap();
    assert_eq!(OwnerMetadataAdoption::decode(&command).unwrap(), a);
    let mut owner = selected(1);
    let boot = owner.bootstrap_command(100000).unwrap();
    assert!(boot.starts_with(b"VBROWN05"));
    apply(&mut owner, 100, boot.clone());
    for id in 1..=32 {
        apply(&mut owner, id, data(a.before(), 1, 1));
    }
    assert_eq!(owner.remaining_operations(), 0);
    assert!(owner
        .validate_proposal(op(300), &command, std::iter::empty())
        .is_ok());
    let original = apply(&mut owner, 300, command.clone()).outcome;
    let RoutedOutcome::ParentAdopted(status) = original else {
        panic!("adoption")
    };
    assert_eq!(status.metadata_operation, op(7));
    assert_eq!(status.metadata_index, a.activation().index);
    assert_ne!(
        status.metadata_index,
        a.activation().publication.imported.source.index
    );
    assert_eq!(owner.grant(), &a.after());
    assert_eq!(owner.local(), group(20));
    assert_eq!(owner.grant().input().epoch, grant().input().epoch);
    assert_eq!(owner.bootstrap_command(100000).unwrap(), boot);
    assert!(
        matches!(apply(&mut owner,1,data(&a.after(),1,1)).outcome,RoutedOutcome::Applied(r) if r.duplicate)
    );
    assert_eq!(owner.application().outbox().count(), 32);
    let cp = owner.checkpoint(100000).unwrap();
    let mut copy = selected(1);
    copy.restore_checkpoint(METADATA_ADOPTING_ROUTED_SCHEMA, owner.applied_index(), &cp)
        .unwrap();
    assert_eq!(copy.grant(), owner.grant());
    assert_eq!(copy.application().outbox().count(), 32);
    assert_eq!(apply(&mut copy, 300, command.clone()).outcome, original);
    let epoch = copy.grant().input().epoch;
    apply(&mut copy, 900, encode_fence(epoch));
    assert_eq!(apply(&mut copy, 300, command).outcome, original);
    let cp = copy.checkpoint(100000).unwrap();
    let mut reopened = selected(1);
    reopened
        .restore_checkpoint(METADATA_ADOPTING_ROUTED_SCHEMA, copy.applied_index(), &cp)
        .unwrap();
    assert_eq!(reopened.grant(), &a.after());
    assert!(reopened.fence().is_some());
    verify_adoption_reads(reopened, status, &a);
}
#[test]
fn adoption_provenance_profiles_capacity_and_atomic_recovery_are_checked() {
    let (a, _) = moved();
    let bytes = a.encode(MAX_METADATA_ADOPTION_BYTES).unwrap();
    assert!(a.encode(bytes.len() - 1).is_err());
    for n in 0..bytes.len() {
        assert!(OwnerMetadataAdoption::decode(&bytes[..n]).is_err());
    }
    // Only self-contained plan/domain/index inconsistencies can be rejected
    // locally; profile/image digests and configurations require quorum provenance.
    for at in [8, 32, 40, 80, 96, 120, 160, 176, 288] {
        let mut bad = bytes.clone();
        bad[at] ^= 1;
        assert!(OwnerMetadataAdoption::decode(&bad).is_err(), "offset {at}");
    }
    let mut wrong = a.activation();
    wrong.publication.imported.source.plan_digest.0[0] ^= 1;
    assert!(
        OwnerMetadataAdoption::new(a.plan().clone(), grant().input().responsibility, wrong)
            .is_err()
    );
    let mut premature = a.activation();
    premature.index = premature.publication.imported.index;
    assert!(OwnerMetadataAdoption::new(
        a.plan().clone(),
        grant().input().responsibility,
        premature
    )
    .is_err());
    assert!(fixture::routed()
        .with_metadata_authority_adoption(0)
        .is_err());
    assert!(fixture::routed()
        .with_metadata_authority_adoption(64)
        .is_err());
    for profile in 0..3 {
        let mut old = match profile {
            0 => fixture::routed(),
            1 => fixture::routed()
                .with_parent_adoption(1)
                .unwrap_or_else(|_| panic!("old")),
            _ => fixture::routed()
                .with_cross_authority_parent_adoption(1)
                .unwrap_or_else(|_| panic!("old")),
        };
        let boot = old.bootstrap_command(100000).unwrap();
        apply(&mut old, 100, boot);
        assert!(old.apply_batch(&[entry(2, 300, bytes.clone())]).is_err());
    }
    let mut owner = selected(1);
    let boot = owner.bootstrap_command(100000).unwrap();
    apply(&mut owner, 100, boot);
    let before = owner.checkpoint(100000).unwrap();
    assert!(owner
        .apply_batch(&[entry(2, 300, bytes.clone()), entry(3, 301, vec![0])])
        .is_err());
    assert_eq!(owner.checkpoint(100000).unwrap(), before);
    assert!(owner
        .validate_proposal(
            op(1),
            &data(&a.after(), 1, 4),
            [(op(300), bytes.as_slice())].into_iter()
        )
        .is_ok());
    apply(&mut owner, 300, bytes.clone());
    apply(&mut owner, 1, data(&a.after(), 1, 4));
    assert!(matches!(
        apply(&mut owner, 301, bytes.clone()).outcome,
        RoutedOutcome::Rejected(RoutingError::WrongOwner)
    ));
    assert!(matches!(
        apply(&mut owner, 1, bytes.clone()).outcome,
        RoutedOutcome::OperationConflict
    ));
    let cp = owner.checkpoint(100000).unwrap();
    let mut empty = selected(1);
    let fresh = empty.checkpoint(100000).unwrap();
    for n in 0..cp.len() {
        assert!(empty
            .restore_checkpoint(5, owner.applied_index(), &cp[..n])
            .is_err());
        assert_eq!(empty.checkpoint(100000).unwrap(), fresh);
    }
    empty
        .restore_checkpoint(5, owner.applied_index(), &cp)
        .unwrap();
    assert_eq!(empty.grant(), &a.after());
    let mut old = fixture::routed()
        .with_cross_authority_parent_adoption(1)
        .unwrap_or_else(|_| panic!("old"));
    assert!(old
        .restore_checkpoint(5, owner.applied_index(), &cp)
        .is_err());
}
#[test]
fn adopted_full_source_performs_later_data_split_without_old_metadata() {
    use voteboat::transfer_publication::*;
    let (a, mut metadata) = moved();
    let mut owner = source(2);
    let boot = owner.bootstrap_command(100000).unwrap();
    apply(&mut owner, 100, boot);
    apply(&mut owner, 1, data(a.before(), 1, 7));
    apply(&mut owner, 2, data(a.before(), 200, 11));
    let adopted = apply(&mut owner, 300, a.encode(100000).unwrap()).outcome;
    assert!(matches!(adopted, RoutedOutcome::ParentAdopted(_)));
    apply(&mut owner, 3, data(&a.after(), 1, 5));
    let mut after = a.after().into_input();
    after.epoch = OwnershipEpoch::new(2).unwrap();
    after.generation = RouteGeneration::new(3).unwrap();
    after.execution = ExecutionMode::Partitioned(vec![
        RouteEntry {
            scope: range(0, 128),
            target: RouteTarget::Group(group(21)),
        },
        RouteEntry {
            scope: range(128, 256),
            target: RouteTarget::Group(group(22)),
        },
    ]);
    let intent =
        TransferIntent::new(a.after(), ResponsibilityManifest::new(after).unwrap()).unwrap();
    assert!(matches!(
        activation::target_apply(&mut metadata, 500, intent.encode(100000).unwrap()),
        MetadataServingOutcome::Directory(DirectoryReceipt {
            outcome: DirectoryOutcome::TransferIntentRecorded,
            ..
        })
    ));
    let cfg = ConfigurationId::new(1).unwrap();
    let freeze = Source::freeze_command(&intent, 100000).unwrap();
    apply(&mut owner, 500, freeze);
    let SourceRead::Freeze(Some(frozen)) = owner
        .read_at(owner.applied_index(), SourceQuery::Freeze)
        .unwrap()
    else {
        panic!("frozen")
    };
    let old = owner.export_target(group(21), 65536).unwrap();
    let (mut targets, ready) = provision_split_targets(&owner, &intent, &frozen, cfg);
    let publication = TransferPublication::new(
        op(500),
        intent.clone(),
        vec![SourceFenceEvidence::from_status(cfg, frozen)
            .unwrap_or_else(|_| panic!("source evidence"))],
        ready,
    )
    .unwrap_or_else(|_| panic!("publication"));
    activation::target_apply(&mut metadata, 501, publication.encode(100000).unwrap());
    let MetadataServingRead::Directory(DirectoryRead::Publication(Some(p))) = metadata
        .read_at(
            metadata.applied_index(),
            MetadataServingQuery::Directory(DirectoryQuery::Publication(op(500))),
        )
        .unwrap()
    else {
        panic!("publication")
    };
    activate_split_targets(&mut targets, &intent, &p);
    let before = owner.checkpoint(100000).unwrap();
    let mut reopened = source(2);
    reopened
        .restore_checkpoint(TRANSFER_SOURCE_SCHEMA, owner.applied_index(), &before)
        .unwrap();
    assert_eq!(reopened.export_target(group(21), 65536).unwrap(), old);
    assert_eq!(
        apply(&mut reopened, 300, a.encode(100000).unwrap()).outcome,
        adopted
    );
    assert_eq!(reopened.routed().application().outbox().count(), 3);
    let SourceRead::MetadataAdoption(Some(status)) = reopened
        .read_at(
            reopened.applied_index(),
            SourceQuery::MetadataAdoption(op(300)),
        )
        .unwrap()
    else {
        panic!("metadata provenance")
    };
    assert_eq!(status.activation, a.activation());
    assert_eq!(status.owner.metadata_index, a.activation().index);
}
fn metadata_source(manifests: Vec<ResponsibilityManifest>) -> MetadataPublishingSource {
    let d = LifecycleDirectory::new(
        Directory::new(
            DirectoryPlan::new(group(1), manifests).unwrap(),
            DirectoryLimits {
                operations: 32,
                history_bytes: 200000,
            },
        )
        .unwrap()
        .with_remaining_transfer()
        .unwrap_or_else(|_| panic!("profile")),
    );
    let bound = d.directory().readiness_requirements().snapshot_bytes;
    MetadataPublishingSource::new(
        MetadataAuthoritySource::new(d, bound).unwrap_or_else(|_| panic!("source")),
    )
    .unwrap()
}
fn move_from(
    mut s: MetadataPublishingSource,
    template: MetadataPublishingSource,
) -> (MetadataServingTarget, MetadataMovePlan) {
    let plan = s.source().plan(group(9)).unwrap();
    let command = s.source().freeze_command(&plan, 200000).unwrap();
    apply(&mut s, 7, command);
    let image = s.source().export(200000).unwrap();
    let mut t = MetadataServingTarget::new(
        template,
        plan.clone(),
        op(7),
        ConfigurationId::new(1).unwrap(),
        ConfigurationId::new(2).unwrap(),
    )
    .unwrap();
    let boot = t.bootstrap_command(40).unwrap();
    apply(&mut t, 7, boot);
    let import = t
        .import_command(&image, ConfigurationId::new(1).unwrap(), 200000)
        .unwrap();
    apply(&mut t, 7, import);
    let command = s
        .publication_command(
            t.status().target.imported.unwrap(),
            ConfigurationId::new(2).unwrap(),
            200000,
        )
        .unwrap();
    apply(&mut s, 7, command);
    let command = t
        .activation_command(s.publication().unwrap(), 200000)
        .unwrap();
    apply(&mut t, 7, command);
    (t, plan)
}
fn tree() -> (ResponsibilityManifest, ResponsibilityManifest) {
    let mut child = grant().into_input();
    child.responsibility.id = ResponsibilityId::new(11).unwrap();
    child.parent = Some(ParentAuthority {
        responsibility: grant().input().responsibility,
        group: group(1),
    });
    child.scope = range(0, 128);
    child.execution = ExecutionMode::Single(group(21));
    let child = ResponsibilityManifest::new(child).unwrap();
    let mut parent = grant().into_input();
    parent.execution = ExecutionMode::Delegated(vec![
        RouteEntry {
            scope: range(0, 128),
            target: RouteTarget::Child(ChildAuthority {
                responsibility: child.input().responsibility,
                group: group(1),
                epoch: child.input().epoch,
            }),
        },
        RouteEntry {
            scope: range(128, 256),
            target: RouteTarget::Group(group(20)),
        },
    ]);
    (ResponsibilityManifest::new(parent).unwrap(), child)
}
fn initialize_metadata(s: &mut MetadataPublishingSource, manifests: &[ResponsibilityManifest]) {
    let boot = s.bootstrap_command(40).unwrap();
    apply(s, 1000, boot);
    for (n, m) in manifests.iter().enumerate() {
        apply(
            s,
            1001 + n as u128,
            DirectoryCommand {
                expected: None,
                manifest: m.clone(),
            }
            .encode(200000)
            .unwrap(),
        );
    }
}
#[test]
fn ordered_local_parent_metadata_and_later_parent_changes_recover_together() {
    use voteboat::reparenting::*;
    let (old, child) = tree();
    let new = alternate_metadata_parent();
    let manifests = vec![old.clone(), new.clone(), child.clone()];
    let template = metadata_source(manifests.clone());
    let mut metadata = template.clone();
    initialize_metadata(&mut metadata, &manifests);
    let plan = ReparentPlan::new(old, new, child.clone()).unwrap();
    apply(&mut metadata, 200, plan.encode(200000).unwrap());
    let first = OwnerParentAdoption {
        metadata_configuration: ConfigurationId::new(1).unwrap(),
        decision: metadata
            .source()
            .directory()
            .unwrap()
            .directory()
            .reparent_status_at(metadata.applied_index(), op(200))
            .unwrap()
            .unwrap(),
    };
    let make = || make_child_owner(&child);
    let mut owner = make();
    let boot = owner.bootstrap_command(100000).unwrap();
    apply(&mut owner, 100, boot);
    let before_hint = RouteHint {
        responsibility: child.input().responsibility,
        group: group(21),
        application: child.input().application,
        scheme: child.input().scheme,
        scope: child.input().scope,
        bucket: 1,
        epoch: child.input().epoch,
        generation: child.input().generation,
    };
    let command = encode_routed(
        before_hint,
        &[1],
        &encode_add(&[1], 7, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap();
    apply(&mut owner, 1, command.clone());
    let first_bytes = first.encode(200000).unwrap();
    let original = apply(&mut owner, 300, first_bytes.clone()).outcome;
    let (mut target, moved) = move_from(metadata, template);
    let adoption = OwnerMetadataAdoption::new(
        moved,
        child.input().responsibility,
        target.status().activation.unwrap(),
    )
    .unwrap();
    let second_bytes = adoption.encode(200000).unwrap();
    let second = apply(&mut owner, 301, second_bytes.clone()).outcome;
    assert_eq!(owner.grant(), &adoption.after());
    let current = adoption.plan().updated_manifests();
    let old = current
        .iter()
        .find(|m| m.input().responsibility == plan.old_parent().input().responsibility)
        .unwrap();
    let new = current
        .iter()
        .find(|m| m.input().responsibility == plan.new_parent().input().responsibility)
        .unwrap();
    let back = ReparentPlan::new(new.clone(), old.clone(), adoption.after()).unwrap();
    apply(&mut target, 201, back.encode(200000).unwrap());
    let MetadataServingRead::Directory(DirectoryRead::Reparent(Some(decision))) = target
        .read_at(
            target.applied_index(),
            MetadataServingQuery::Directory(DirectoryQuery::Reparent(op(201))),
        )
        .unwrap()
    else {
        panic!("new decision")
    };
    let third = OwnerParentAdoption {
        metadata_configuration: ConfigurationId::new(2).unwrap(),
        decision,
    };
    apply(&mut owner, 302, third.encode(200000).unwrap());
    assert_eq!(owner.grant(), &third.after());
    let cp = owner.checkpoint(200000).unwrap();
    let mut copy = make();
    copy.restore_checkpoint(5, owner.applied_index(), &cp)
        .unwrap();
    assert_eq!(copy.grant(), owner.grant());
    assert_eq!(apply(&mut copy, 300, first_bytes).outcome, original);
    assert_eq!(apply(&mut copy, 301, second_bytes).outcome, second);
    assert!(matches!(apply(&mut copy,1,command).outcome,RoutedOutcome::Applied(r) if r.duplicate));
    assert_eq!(copy.application().outbox().count(), 1);
}
#[cfg(feature = "native")]
#[test]
fn native_cache_accepts_only_checked_metadata_shape_and_refuses_partial_paths() {
    use voteboat::native::routing::NativeManifestCache;
    let (parent, child) = tree();
    let before = vec![parent, child];
    let template = metadata_source(before.clone());
    let mut source = template.clone();
    initialize_metadata(&mut source, &before);
    let (target, plan) = move_from(source, template);
    let after = plan.updated_manifests();
    let observation = OwnerMetadataAdoption::new(
        plan.clone(),
        before[1].input().responsibility,
        target.status().activation.unwrap(),
    )
    .unwrap();
    assert_eq!(observation.after(), after[1]);
    let limits = ManifestCacheLimits {
        manifests: 8,
        bytes: MAX_CACHE_BYTES,
    };
    let mut old = NativeManifestCache::new(limits).unwrap();
    old.admit(before[0].clone()).unwrap();
    assert_eq!(
        old.admit(after[0].clone()).unwrap_err().0,
        RoutingError::IdentityChange
    );
    for child_first in [false, true] {
        let mut cache = NativeManifestCache::new(limits)
            .unwrap()
            .with_metadata_authority_moves()
            .unwrap_or_else(|_| panic!("cache"));
        for m in &before {
            cache.admit(m.clone()).unwrap();
        }
        let index = usize::from(child_first);
        cache.admit(after[index].clone()).unwrap();
        assert!(resolve(
            &cache,
            &fixture::Policy,
            before[0].input().responsibility,
            &[1],
            3
        )
        .is_err());
        cache.admit(after[1 - index].clone()).unwrap();
        assert_eq!(
            resolve(
                &cache,
                &fixture::Policy,
                before[0].input().responsibility,
                &[1],
                3
            )
            .unwrap()
            .group,
            group(21)
        );
        assert_eq!(
            cache.admit(before[1].clone()).unwrap_err().0,
            RoutingError::StaleGeneration
        );
        assert!(cache.with_metadata_authority_moves().is_err());
    }
    for kind in 0..5 {
        let mut bad = after[1].clone().into_input();
        match kind {
            0 => bad.epoch = OwnershipEpoch::new(2).unwrap(),
            1 => bad.execution = ExecutionMode::Single(group(99)),
            2 => bad.generation = RouteGeneration::new(3).unwrap(),
            3 => bad.application.version += 1,
            _ => bad.state = ResponsibilityState::Fenced,
        }
        let bad = ResponsibilityManifest::new(bad).unwrap();
        assert!(!bad.moves_metadata_from(&before[1]));
        let mut cache = NativeManifestCache::new(limits)
            .unwrap()
            .with_metadata_authority_moves()
            .unwrap_or_else(|_| panic!("cache"));
        cache.admit(before[1].clone()).unwrap();
        let usage = cache.usage();
        assert!(cache.admit(bad).is_err());
        assert_eq!(cache.usage(), usage);
    }
}
#[cfg(feature = "native")]
#[test]
fn native_metadata_owner_adoption_cuts_preserve_data_and_resume_exact_control() {
    use support::{Fault, ModelIo};
    use voteboat::native::log_store::*;
    let (a, _) = moved();
    let command = a.encode(200000).unwrap();
    let mut owner = selected(1);
    let entries = vec![
        entry(1, 100, owner.bootstrap_command(100000).unwrap()),
        entry(2, 1, data(a.before(), 1, 7)),
        entry(3, 300, command.clone()),
    ];
    owner.apply_batch(&entries).unwrap();
    let expected = owner.parent_adoption(op(300)).unwrap();
    let limits = LogLimits::default();
    let seed = || {
        let io = ModelIo::default();
        let mut log = NativeLogStore::create(io.clone(), support::identity(1), limits).unwrap();
        support::append(
            &mut log,
            vec![LogMutation::Create(support::bootstrap(20, 3))],
        );
        let state = log.state(group(20)).unwrap();
        support::append(
            &mut log,
            vec![support::update(
                &state,
                1,
                2,
                Some(Suffix {
                    from: 1,
                    entries: entries[..2].to_vec(),
                }),
            )],
        );
        (io, log)
    };
    let (_, log) = seed();
    let mutation = support::update(
        &log.state(group(20)).unwrap(),
        1,
        3,
        Some(Suffix {
            from: 3,
            entries: entries[2..].to_vec(),
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
        let state = log.state(group(20)).unwrap();
        let complete = state.commit_index == 3;
        outcomes[usize::from(complete)] = true;
        let mut copy = selected(1);
        copy.apply_batch(
            &state
                .entries
                .into_iter()
                .filter(|e| e.index <= state.commit_index)
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let expected_grant = if complete {
            a.after()
        } else {
            a.before().clone()
        };
        assert_eq!(copy.grant(), &expected_grant);
        assert_eq!(copy.application().outbox().count(), 1);
        assert!(
            matches!(apply(&mut copy,300,command.clone()).outcome,RoutedOutcome::ParentAdopted(s) if s==expected)
        );
        let cp = copy.checkpoint(200000).unwrap();
        let mut reopened = selected(1);
        reopened
            .restore_checkpoint(5, copy.applied_index(), &cp)
            .unwrap();
        assert_eq!(reopened.grant(), &a.after());
        assert!(
            matches!(apply(&mut reopened,1,data(&a.after(),1,7)).outcome,RoutedOutcome::Applied(r) if r.duplicate)
        );
    }
    assert_eq!(outcomes, [true, true]);
}

fn verify_adoption_reads(reopened: Owner, status: ParentGrantStatus, a: &OwnerMetadataAdoption) {
    let reads = RoutedControlReads::new(reopened);
    assert_eq!(
        reads
            .read_at(
                reads.applied_index(),
                RoutedControlQuery::ParentAdoption(op(300))
            )
            .unwrap(),
        RoutedControlRead::ParentAdoption(Some(status))
    );
    let query = RoutedControlQuery::MetadataAdoption(op(300));
    let value = reads.read_at(reads.applied_index(), query.clone()).unwrap();
    assert_eq!(
        value,
        RoutedControlRead::MetadataAdoption(Some(MetadataGrantStatus {
            owner: status,
            activation: a.activation(),
        }))
    );
    assert_eq!(reads.read_result_bytes(&value, 0).unwrap(), 0);
    assert_eq!(
        reads.read_result_bound(&query).unwrap(),
        std::mem::size_of::<RoutedControlRead<i64>>()
    );
}

type SplitTarget = TransferTarget<BucketCounter<fixture::Policy>, fixture::Policy>;
fn provision_split_targets(
    owner: &Source,
    intent: &TransferIntent,
    frozen: &SourceFreezeStatus,
    cfg: ConfigurationId,
) -> (
    Vec<SplitTarget>,
    Vec<voteboat::transfer_publication::TargetReadyEvidence>,
) {
    use voteboat::transfer_publication::*;
    let mut targets = Vec::new();
    let mut ready = Vec::new();
    for (g, scope) in [(21, range(0, 128)), (22, range(128, 256))] {
        let mut target = TransferTarget::new(
            group(g),
            op(500),
            intent.clone(),
            BucketCounter::new(scope, fixture::Policy, fixture::bucket_limits()).unwrap(),
            fixture::Policy,
            TargetLimits {
                import_bytes: 65536,
                application_checkpoint_bytes: fixture::bucket_limits().checkpoint_bound().unwrap(),
            },
        )
        .unwrap_or_else(|_| panic!("target"));
        let boot = target.bootstrap_command(100000).unwrap();
        apply(&mut target, 500, boot);
        let import = TargetImport::new(
            op(500),
            intent.clone(),
            group(g),
            vec![SourceImport {
                fence: frozen.fence,
                configuration: cfg,
                image: owner.export_target(group(g), 65536).unwrap(),
                digest: frozen
                    .exports
                    .iter()
                    .find(|e| e.target == group(g))
                    .unwrap()
                    .digest,
            }],
        )
        .unwrap_or_else(|_| panic!("import"));
        let cmd = target.import_command(&import, 100000).unwrap();
        apply(&mut target, 500, cmd);
        ready.push(
            TargetReadyEvidence::from_status(cfg, target.status())
                .unwrap_or_else(|_| panic!("ready")),
        );
        targets.push(target);
    }
    (targets, ready)
}

fn activate_split_targets(
    targets: &mut [SplitTarget],
    intent: &TransferIntent,
    p: &voteboat::transfer_publication::TransferPublicationStatus,
) {
    for (target, (g, key, operation, delta)) in
        targets.iter_mut().zip([(21, 1, 1, 7), (22, 200, 2, 11)])
    {
        let cmd = target
            .activation_command(
                &TargetActivation {
                    metadata_configuration: ConfigurationId::new(2).unwrap(),
                    decision: p.clone(),
                },
                100000,
            )
            .unwrap();
        apply(target, 500, cmd);
        let input = intent.after().input();
        let hint = RouteHint {
            responsibility: input.responsibility,
            group: group(g),
            application: input.application,
            scheme: input.scheme,
            scope: if g == 21 {
                range(0, 128)
            } else {
                range(128, 256)
            },
            bucket: key as u16,
            epoch: input.epoch,
            generation: input.generation,
        };
        let command = encode_routed(
            hint,
            &[key],
            &encode_add(&[key], delta, b"outbox", 1024).unwrap(),
            4096,
        )
        .unwrap();
        assert!(
            matches!(apply(target,operation,command).outcome,TargetOutcome::Applied(r) if r.duplicate)
        );
    }
}

fn make_child_owner(child: &ResponsibilityManifest) -> Owner {
    RoutedApplication::new(
        group(21),
        child.clone(),
        BucketCounter::new(range(0, 128), fixture::Policy, fixture::bucket_limits()).unwrap(),
        fixture::Policy,
        RoutedLimits {
            operations: 32,
            semantic_bytes: 8192,
            payload_bytes: 1024,
            inner_checkpoint_bytes: fixture::bucket_limits().checkpoint_bound().unwrap(),
        },
    )
    .unwrap_or_else(|_| panic!("owner"))
    .with_metadata_authority_adoption(3)
    .unwrap_or_else(|_| panic!("profile"))
}

fn alternate_metadata_parent() -> ResponsibilityManifest {
    let mut new = grant().into_input();
    new.responsibility.id = ResponsibilityId::new(30).unwrap();
    new.execution = ExecutionMode::Delegated(vec![
        RouteEntry {
            scope: range(0, 128),
            target: RouteTarget::Vacant,
        },
        RouteEntry {
            scope: range(128, 256),
            target: RouteTarget::Group(group(40)),
        },
    ]);
    ResponsibilityManifest::new(new).unwrap()
}
