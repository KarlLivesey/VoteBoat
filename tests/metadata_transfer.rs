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
use fixture::{entry, grant, group, op, range};
use voteboat::{
    application::*, directory::*, identity::*, log::*, metadata_transfer::*, routing::*,
    transfer::*,
};
fn directory(operations: usize) -> LifecycleDirectory {
    let d = LifecycleDirectory::new(
        Directory::new(
            DirectoryPlan::new(group(1), vec![grant()]).unwrap(),
            DirectoryLimits {
                operations,
                history_bytes: 100000,
            },
        )
        .unwrap()
        .with_remaining_transfer()
        .unwrap_or_else(|_| panic!("profile")),
    );
    d
}
fn fresh(operations: usize) -> MetadataAuthoritySource {
    let d = directory(operations);
    let limit = d.directory().readiness_requirements().snapshot_bytes;
    MetadataAuthoritySource::new(d, limit).unwrap_or_else(|_| panic!("source"))
}
fn commit(a: &mut MetadataAuthoritySource, id: u128, b: Vec<u8>) -> MetadataSourceOutcome {
    a.apply_batch(&[entry(a.applied_index() + 1, id, b)])
        .unwrap()
        .remove(0)
        .outcome
}
fn ready(operations: usize) -> (MetadataAuthoritySource, Vec<LogEntry>) {
    let mut a = fresh(operations);
    let boot = a.bootstrap_command(100000).unwrap();
    let publish = DirectoryCommand {
        expected: None,
        manifest: grant(),
    }
    .encode(100000)
    .unwrap();
    let entries = vec![entry(1, 1000, boot), entry(2, 1001, publish)];
    a.apply_batch(&entries).unwrap();
    (a, entries)
}
fn read(a: &MetadataAuthoritySource) -> MetadataSourceRead {
    a.read_at(
        a.applied_index(),
        MetadataSourceQuery::Directory(DirectoryQuery::Manifest(grant().input().responsibility)),
    )
    .unwrap()
}
#[test]
fn source_fence_exports_exact_old_authority_and_keeps_original_history() {
    let (mut a, _) = ready(2);
    assert!(a.directory().is_some());
    let p = a.plan(group(9)).unwrap();
    let command = a.freeze_command(&p, 100000).unwrap();
    assert!(a
        .validate_proposal(op(7), &command, std::iter::empty())
        .is_ok());
    let status = match commit(&mut a, 7, command.clone()) {
        MetadataSourceOutcome::Frozen(s) => s,
        _ => panic!("fence"),
    };
    assert_eq!(status.index, 3);
    assert!(a.directory().is_none());
    assert_eq!(read(&a), MetadataSourceRead::Fenced);
    let image = a.export(status.image_bytes).unwrap();
    assert!(a.export(status.image_bytes - 1).is_err());
    assert_eq!(image.plan(), &p);
    assert_eq!(image.status(), status);
    let mut old = directory(2);
    old.restore_checkpoint(status.directory_schema, status.index, image.bytes())
        .unwrap();
    assert_eq!(
        old.read_at(
            status.index,
            DirectoryQuery::Manifest(grant().input().responsibility)
        )
        .unwrap(),
        DirectoryRead::Manifest(Some(grant()))
    );
    let receipt = old
        .apply_batch(&[entry(
            4,
            1001,
            DirectoryCommand {
                expected: None,
                manifest: grant(),
            }
            .encode(100000)
            .unwrap(),
        )])
        .unwrap()
        .remove(0);
    assert!(receipt.duplicate);
    assert_eq!(
        receipt.outcome,
        DirectoryOutcome::Published(RouteGeneration::new(1).unwrap())
    );
    let outcome = commit(&mut a, 7, command.clone());
    assert_eq!(outcome, MetadataSourceOutcome::Frozen(status));
    let update = DirectoryCommand {
        expected: Some(RouteGeneration::new(1).unwrap()),
        manifest: grant(),
    }
    .encode(100000)
    .unwrap();
    assert!(a
        .validate_proposal(op(8), &update, std::iter::empty())
        .is_err());
    assert_eq!(commit(&mut a, 8, update), MetadataSourceOutcome::Fenced);
    assert_eq!(a.export(100000).unwrap(), image);
    let cp = a
        .checkpoint(a.readiness_requirements().snapshot_bytes)
        .unwrap();
    let mut b = fresh(2);
    b.restore_checkpoint(METADATA_SOURCE_SCHEMA, a.applied_index(), &cp)
        .unwrap();
    assert_eq!(b.status(), Some(status));
    assert_eq!(b.export(100000).unwrap(), image);
    assert_eq!(
        commit(&mut b, 7, command),
        MetadataSourceOutcome::Frozen(status)
    );
}
#[test]
fn plans_validate_closure_and_preserve_data_ownership() {
    let mut parent = grant().into_input();
    let mut child = grant().into_input();
    child.responsibility = ResponsibilityIdentity {
        id: ResponsibilityId::new(30).unwrap(),
        incarnation: ResponsibilityIncarnation::new(1).unwrap(),
    };
    child.parent = Some(ParentAuthority {
        responsibility: parent.responsibility,
        group: group(1),
    });
    child.scope = range(0, 128);
    child.execution = ExecutionMode::Single(group(30));
    parent.execution = ExecutionMode::Delegated(vec![
        RouteEntry {
            scope: range(0, 128),
            target: RouteTarget::Child(ChildAuthority {
                responsibility: child.responsibility,
                group: group(1),
                epoch: child.epoch,
            }),
        },
        RouteEntry {
            scope: range(128, 256),
            target: RouteTarget::Group(group(20)),
        },
    ]);
    let parent = ResponsibilityManifest::new(parent).unwrap();
    let child = ResponsibilityManifest::new(child).unwrap();
    let p = MetadataMovePlan::new(group(1), group(9), vec![child.clone(), parent.clone()]).unwrap();
    let encoded = p.encode(MAX_METADATA_PLAN_BYTES).unwrap();
    assert_eq!(MetadataMovePlan::decode(&encoded).unwrap(), p);
    for end in 0..encoded.len() {
        assert!(MetadataMovePlan::decode(&encoded[..end]).is_err());
    }
    for moved in p.updated_manifests() {
        let before = p
            .manifests()
            .iter()
            .find(|m| m.input().responsibility == moved.input().responsibility)
            .unwrap()
            .input();
        assert_eq!(moved.input().authority, group(9));
        assert_eq!(moved.input().epoch, before.epoch);
        assert_eq!(moved.input().generation.get(), before.generation.get() + 1);
        if let ExecutionMode::Single(g) = before.execution {
            assert_eq!(moved.input().execution, ExecutionMode::Single(g));
        }
    }
    assert!(MetadataMovePlan::new(group(1), group(9), vec![parent.clone()]).is_err());
    assert!(MetadataMovePlan::new(group(1), group(9), vec![child.clone()]).is_err());
    for target in [
        group(1),
        group(20),
        group(30),
        GroupIdentity {
            id: group(1).id,
            incarnation: GroupIncarnation::new(2).unwrap(),
        },
    ] {
        assert!(
            MetadataMovePlan::new(group(1), target, vec![parent.clone(), child.clone()]).is_err()
        );
    }
    let mut overflow = child.clone().into_input();
    overflow.generation = RouteGeneration::new(u64::MAX).unwrap();
    assert!(MetadataMovePlan::new(
        group(1),
        group(9),
        vec![parent, ResponsibilityManifest::new(overflow).unwrap()]
    )
    .is_err());
}
#[test]
fn pending_changes_stale_plans_and_open_lifecycles_cannot_freeze() {
    let (mut a, _) = ready(8);
    let old = a.plan(group(9)).unwrap();
    let mut next = grant().into_input();
    next.generation = RouteGeneration::new(2).unwrap();
    let update = DirectoryCommand {
        expected: Some(RouteGeneration::new(1).unwrap()),
        manifest: ResponsibilityManifest::new(next).unwrap(),
    }
    .encode(100000)
    .unwrap();
    let freeze = a.freeze_command(&old, 100000).unwrap();
    assert!(a
        .validate_proposal(op(7), &freeze, std::iter::once((op(6), update.as_slice())))
        .is_err());
    commit(&mut a, 6, update);
    assert_eq!(commit(&mut a, 7, freeze), MetadataSourceOutcome::Conflict);
    assert!(a.status().is_none());
    let intent = fixture::intent();
    let (mut pending, _) = ready(8);
    commit(&mut pending, 20, intent.encode(100000).unwrap());
    assert!(pending.plan(group(9)).is_err());
    let command = pending.freeze_command(&old, 100000).unwrap();
    assert!(pending
        .validate_proposal(op(7), &command, std::iter::empty())
        .is_err());
    assert_eq!(
        commit(&mut pending, 7, command),
        MetadataSourceOutcome::Conflict
    );
    let b = a
        .freeze_command(&a.plan(group(9)).unwrap(), 100000)
        .unwrap();
    assert!(a
        .validate_proposal(op(1000), &b, std::iter::empty())
        .is_err());
}
#[test]
fn checkpoints_bind_profile_fence_digest_and_original_bootstrap() {
    let (mut a, _) = ready(2);
    let p = a.plan(group(9)).unwrap();
    let command = a.freeze_command(&p, 100000).unwrap();
    commit(&mut a, 7, command);
    let limit = a.readiness_requirements().snapshot_bytes;
    let bytes = a.checkpoint(limit).unwrap();
    assert_eq!(
        bytes.len(),
        180 + a.export(100000).unwrap().bytes().len() + a.freeze_command(&p, 100000).unwrap().len()
    );
    assert!(a.checkpoint(bytes.len() - 1).is_err());
    let before = fresh(2).checkpoint(limit).unwrap();
    let mut b = fresh(2);
    for end in 0..bytes.len() {
        assert!(b
            .restore_checkpoint(METADATA_SOURCE_SCHEMA, 3, &bytes[..end])
            .is_err());
        assert_eq!(b.checkpoint(limit).unwrap(), before);
    }
    for offset in [8, 40, 56, 72, 80, 92, bytes.len() - 1] {
        let mut changed = bytes.clone();
        changed[offset] ^= 1;
        assert!(b
            .restore_checkpoint(METADATA_SOURCE_SCHEMA, 3, &changed)
            .is_err());
    }
    assert!(fresh(3)
        .restore_checkpoint(METADATA_SOURCE_SCHEMA, 3, &bytes)
        .is_err());
    assert!(b.restore_checkpoint(999, 3, &bytes).is_err());
}

#[cfg(feature = "native")]
#[test]
fn interrupted_native_metadata_fence_recovers_old_or_complete_image() {
    use support::{Fault, ModelIo};
    use voteboat::native::log_store::*;
    let (mut a, mut entries) = ready(2);
    let plan = a.plan(group(9)).unwrap();
    let command = a.freeze_command(&plan, 100000).unwrap();
    entries.push(entry(3, 7, command.clone()));
    a.apply_batch(&entries[2..]).unwrap();
    let expected = a.export(100000).unwrap();
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
        &log.state(group(1)).unwrap(),
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
        let state = log.state(group(1)).unwrap();
        let mut recovered = fresh(2);
        recovered
            .apply_batch(
                &state
                    .entries
                    .into_iter()
                    .filter(|e| e.index <= state.commit_index)
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        if state.commit_index == 2 {
            outcomes[0] = true;
            assert!(matches!(read(&recovered), MetadataSourceRead::Directory(_)));
            assert!(recovered.export(100000).is_err());
        } else {
            outcomes[1] = true;
            assert_eq!(state.commit_index, 3);
            assert_eq!(read(&recovered), MetadataSourceRead::Fenced);
            assert_eq!(recovered.export(100000).unwrap(), expected);
        }
        assert!(matches!(
            commit(&mut recovered, 7, command.clone()),
            MetadataSourceOutcome::Frozen(_)
        ));
        assert_eq!(recovered.export(100000).unwrap(), expected);
    }
    assert_eq!(outcomes, [true, true]);
}

#[test]
fn source_selection_bootstrap_and_failed_batches_preserve_atomicity() {
    let d = directory(2);
    assert!(MetadataAuthoritySource::new(d.clone(), 1).is_err());
    let mut unselected = fresh(2);
    let raw = d.directory().bootstrap_command(100000).unwrap();
    assert!(unselected
        .validate_proposal(op(1), &raw, std::iter::empty())
        .is_err());
    let (mut a, _) = ready(8);
    let before = a
        .checkpoint(a.readiness_requirements().snapshot_bytes)
        .unwrap();
    let plan = a.plan(group(9)).unwrap();
    let mut invalid = a.freeze_command(&plan, 100000).unwrap();
    invalid.pop();
    let mut next = grant().into_input();
    next.generation = RouteGeneration::new(2).unwrap();
    let publish = DirectoryCommand {
        expected: Some(RouteGeneration::new(1).unwrap()),
        manifest: ResponsibilityManifest::new(next).unwrap(),
    }
    .encode(100000)
    .unwrap();
    assert!(a
        .apply_batch(&[entry(3, 10, publish), entry(4, 11, invalid)])
        .is_err());
    assert_eq!(
        a.checkpoint(a.readiness_requirements().snapshot_bytes)
            .unwrap(),
        before
    );
    let mut no_bootstrap = before.clone();
    no_bootstrap[56..72].fill(0);
    assert!(unselected
        .restore_checkpoint(METADATA_SOURCE_SCHEMA, 2, &no_bootstrap)
        .is_err());
    assert!(a.validate_group(group(9)).is_err());
    assert!(a.read_at(3, MetadataSourceQuery::Status).is_err());
    let live = directory(2);
    let mut live = live.clone();
    live.apply_batch(&[entry(1, 1, raw)]).unwrap();
    assert!(MetadataAuthoritySource::new(live, MAX_METADATA_IMAGE_BYTES).is_err());
}

#[test]
fn rejected_control_ids_survive_recovery_and_do_not_exhaust_the_success_reserve() {
    let (mut a, _) = ready(2);
    let plan = a.plan(group(9)).unwrap();
    let mut stale = grant().into_input();
    stale.generation = RouteGeneration::new(2).unwrap();
    let stale = MetadataMovePlan::new(
        group(1),
        group(9),
        vec![ResponsibilityManifest::new(stale).unwrap()],
    )
    .unwrap();
    let bad = a.freeze_command(&stale, 100000).unwrap();
    let good = a.freeze_command(&plan, 100000).unwrap();
    assert_eq!(
        commit(&mut a, 7, bad.clone()),
        MetadataSourceOutcome::Conflict
    );
    assert_eq!(
        commit(&mut a, 7, good.clone()),
        MetadataSourceOutcome::Conflict
    );
    assert!(a.status().is_none());
    assert_eq!(
        commit(&mut a, 9, bad.clone()),
        MetadataSourceOutcome::Conflict
    );
    let cp = a
        .checkpoint(a.readiness_requirements().snapshot_bytes)
        .unwrap();
    assert!(a
        .apply_batch(&[entry(a.applied_index() + 1, 11, bad)])
        .is_err());
    assert_eq!(
        a.checkpoint(a.readiness_requirements().snapshot_bytes)
            .unwrap(),
        cp
    );
    let mut recovered = fresh(2);
    recovered
        .restore_checkpoint(METADATA_SOURCE_SCHEMA, a.applied_index(), &cp)
        .unwrap();
    assert_eq!(
        commit(&mut recovered, 7, good.clone()),
        MetadataSourceOutcome::Conflict
    );
    assert!(matches!(
        commit(&mut recovered, 10, good.clone()),
        MetadataSourceOutcome::Frozen(_)
    ));
    let image = recovered.export(100000).unwrap();
    assert_eq!(image.rejected_operations(), &[(op(7), 3), (op(9), 5)]);
    let cp = recovered
        .checkpoint(recovered.readiness_requirements().snapshot_bytes)
        .unwrap();
    let mut final_copy = fresh(2);
    final_copy
        .restore_checkpoint(METADATA_SOURCE_SCHEMA, recovered.applied_index(), &cp)
        .unwrap();
    assert_eq!(final_copy.export(100000).unwrap(), image);
    assert_eq!(
        commit(&mut final_copy, 7, good),
        MetadataSourceOutcome::Conflict
    );
    assert_eq!(final_copy.export(100000).unwrap(), image);
}

#[path = "metadata_transfer/target.rs"]
mod target;

#[path = "metadata_transfer/activation.rs"]
mod activation;

#[path = "metadata_transfer/adoption.rs"]
mod adoption;

#[path = "metadata_transfer/locators.rs"]
mod locators;

#[path = "metadata_transfer/owner_locators.rs"]
mod owner_locators;

#[path = "metadata_transfer/repeated.rs"]
mod repeated;
