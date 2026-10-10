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
use voteboat::{deletion::DeletionIntent, transfer::*};
#[cfg(feature = "native")]
#[path = "cancellation_faults.rs"]
mod faults;

fn fresh(ops: usize) -> Directory {
    directory(ops)
        .with_creation_cancellation()
        .unwrap_or_else(|_| panic!("schema16"))
}

fn cancellation(p: &NamespacePlan) -> Vec<u8> {
    CancelGroupCreation::from_status(&p.creation)
        .encode(GROUP_CREATION_CANCELLATION_BYTES)
        .unwrap()
}

fn recover(d: &Directory) -> Directory {
    let mut reopened = fresh(d.limits().operations);
    reopened
        .restore_checkpoint(
            d.schema_version(),
            d.applied_index(),
            &d.checkpoint(1000000).unwrap(),
        )
        .unwrap();
    reopened
}

#[test]
fn cancellation_binding_codec_and_schema_refuse_before_mutation() {
    let (mut d, p) = reserve_directory(fresh(16));
    let command = cancellation(&p);
    let c = CancelGroupCreation::decode(&command).unwrap();
    assert_eq!(c, CancelGroupCreation::from_status(&p.creation));
    assert!(c.encode(command.len() - 1).is_err());
    for len in 0..command.len() {
        assert!(CancelGroupCreation::decode(&command[..len]).is_err());
    }
    let mut extra = command.clone();
    extra.push(0);
    assert!(CancelGroupCreation::decode(&extra).is_err());
    for field in 0..4 {
        let mut wrong = c;
        match field {
            0 => wrong.creation = OperationId::new(900).unwrap(),
            1 => wrong.creation_index += 1,
            2 => wrong.group.id = GroupId::new(101).unwrap(),
            _ => wrong.group.incarnation = GroupIncarnation::new(2).unwrap(),
        }
        assert_eq!(
            commit(&mut d, 20 + field, wrong.encode(56).unwrap()).outcome,
            DirectoryOutcome::TransferEvidenceMismatch
        );
        assert!(d
            .group_creation_at(d.applied_index(), group(100))
            .unwrap()
            .is_some());
    }
    let (mut old, _) = reserve(16);
    assert!(old.apply_batch(&[entry(4, 5, command)]).is_err());
    assert!(old
        .group_creation_cancellation_at(3, p.creation.operation)
        .is_err());
    let image = d.checkpoint(1000000).unwrap();
    assert!(old
        .restore_checkpoint(16, d.applied_index(), &image)
        .is_err());
    assert!(old
        .restore_checkpoint(3, d.applied_index(), &image)
        .is_err());
    assert_eq!(d.schema_version(), CREATION_CANCELLATION_DIRECTORY_SCHEMA);
}

#[test]
fn cancelled_ready_target_cannot_publish_or_serve_and_original_receipt_recovers() {
    let (mut d, p) = reserve_directory(fresh(16));
    let parent = d.manifest(identity_of(1)).unwrap().clone();
    let mut t = target(&p);
    let publication = ready(&mut t); // readiness receipt can already exist in flight
    let command = cancellation(&p);
    assert_eq!(
        commit(&mut d, 4, command.clone()).outcome,
        DirectoryOutcome::CreationCancelled
    );
    let original = d
        .group_creation_cancellation_at(4, p.creation.operation)
        .unwrap()
        .unwrap();
    assert!(d
        .group_creation_cancellation_at(5, p.creation.operation)
        .is_err());
    assert!(d
        .group_creation_at(p.creation.index, group(100))
        .unwrap()
        .is_none());
    let mut d = recover(&d); // lost response; retry exact operation after reopen
    assert!(commit(&mut d, 4, command.clone()).duplicate);
    assert_eq!(
        d.group_creation_cancellation_at(d.applied_index(), p.creation.operation)
            .unwrap(),
        Some(original)
    );
    assert_eq!(
        commit(&mut d, 5, publication.encode(1024).unwrap()).outcome,
        DirectoryOutcome::TransferEvidenceMismatch
    );
    assert!(d
        .namespace_publication_at(d.applied_index(), p.creation.operation)
        .unwrap()
        .is_none());
    assert_eq!(
        t.read_at(t.applied_index(), query()).unwrap(),
        NamespaceRead::NotActive
    );
    let (op, bytes) = data(80, 4);
    assert!(t.validate_proposal(op, &bytes, std::iter::empty()).is_err());
    assert_eq!(
        commit(&mut t, 80, bytes).outcome,
        NamespaceOutcome::NotActive
    );
    assert_eq!(d.manifest(identity_of(1)), Some(&parent));
    assert!(d.manifest(identity_of(50)).is_none());
    assert_eq!(
        commit(&mut d, 6, command).outcome,
        DirectoryOutcome::TransferEvidenceMismatch
    );
    let retry = p.creation.intent.encode(MAX_GROUP_CREATION_BYTES).unwrap();
    assert_eq!(
        commit(&mut d, 3, retry.clone()).outcome,
        DirectoryOutcome::CreationReserved
    );
    assert_eq!(
        commit(&mut d, 7, retry).outcome,
        DirectoryOutcome::CreationConflict
    );
    let mut reused = p.creation.intent.clone();
    reused.bootstrap.group.incarnation = GroupIncarnation::new(2).unwrap();
    assert_eq!(
        commit(&mut d, 8, reused.encode(MAX_GROUP_CREATION_BYTES).unwrap()).outcome,
        DirectoryOutcome::CreationConflict
    );
    let view = LifecycleDirectory::new(recover(&d));
    let q = DirectoryQuery::CreationCancellation(p.creation.operation);
    let result = view.read_at(view.applied_index(), q).unwrap();
    assert_eq!(result, DirectoryRead::CreationCancellation(Some(original)));
    assert_eq!(view.read_result_bytes(&result, 0).unwrap(), 0);
    assert_eq!(
        view.read_result_bound(&q).unwrap(),
        std::mem::size_of::<DirectoryRead>()
    );
}

#[test]
fn published_and_activated_namespace_rejects_cancellation_after_recovery() {
    let (mut d, p) = reserve_directory(fresh(16));
    let mut t = target(&p);
    let pubn = ready(&mut t);
    commit(&mut d, 4, pubn.encode(1024).unwrap());
    let status = d
        .namespace_publication_at(d.applied_index(), p.creation.operation)
        .unwrap()
        .unwrap();
    let activate = t.activation_command(&status, 2000).unwrap();
    assert_eq!(
        commit(&mut t, 4, activate).outcome,
        NamespaceOutcome::Activated
    );
    let mut d = recover(&d);
    assert_eq!(
        commit(&mut d, 5, cancellation(&p)).outcome,
        DirectoryOutcome::TransferEvidenceMismatch
    );
    assert!(d
        .group_creation_cancellation_at(d.applied_index(), p.creation.operation)
        .unwrap()
        .is_none());
    assert!(d
        .group_creation_at(d.applied_index(), group(100))
        .unwrap()
        .is_some());
    let (_, bytes) = data(81, 7);
    commit(&mut t, 81, bytes);
    assert_eq!(
        t.read_at(t.applied_index(), query()).unwrap(),
        NamespaceRead::Data(RoutedRead::Served(7))
    );
}

#[test]
fn cancellation_uses_reserved_capacity_and_releases_parent_deletion_lock() {
    let (mut full, p) = reserve_directory(fresh(3));
    assert_eq!(full.remaining_operations(), 0);
    let bytes = cancellation(&p);
    let op = OperationId::new(4).unwrap();
    assert!(full
        .validate_proposal(op, &bytes, std::iter::empty())
        .is_ok());
    assert!(full
        .validate_proposal(
            OperationId::new(5).unwrap(),
            &bytes,
            [(op, bytes.as_slice())].into_iter()
        )
        .is_err());
    assert_eq!(
        commit(&mut full, 4, bytes).outcome,
        DirectoryOutcome::CreationCancelled
    );
    assert_eq!(full.remaining_operations(), 0);
    assert_eq!(full.reserved_publication_bytes(), 0);
    assert_eq!(recover(&full).remaining_operations(), 0);

    let (mut d, p) = reserve_directory(fresh(16));
    let deletion = DeletionIntent {
        before: d.manifest(identity_of(1)).unwrap().clone(),
    }
    .encode(100000)
    .unwrap();
    assert_eq!(
        commit(&mut d, 4, deletion.clone()).outcome,
        DirectoryOutcome::LifecycleBusy
    );
    commit(&mut d, 5, cancellation(&p));
    let mut d = recover(&d);
    assert_eq!(
        commit(&mut d, 6, deletion).outcome,
        DirectoryOutcome::DeletionIntentRecorded
    );
}

#[test]
fn competing_control_admission_and_failed_batches_preserve_the_original_reservation() {
    for cancel_first in [true, false] {
        let (mut d, p) = reserve_directory(fresh(3));
        let cancel = cancellation(&p);
        let publish = ready(&mut target(&p)).encode(1024).unwrap();
        let (first, second) = if cancel_first {
            (cancel, publish)
        } else {
            (publish, cancel)
        };
        let op = OperationId::new(4).unwrap();
        assert!(d.validate_proposal(op, &first, std::iter::empty()).is_ok());
        assert_eq!(
            d.validate_proposal(
                OperationId::new(5).unwrap(),
                &second,
                [(op, first.as_slice())].into_iter()
            ),
            Err(ApplicationError::DedupCapacity)
        );
        let before = d.checkpoint(1000000).unwrap();
        assert!(d
            .apply_batch(&[entry(4, 4, first.clone()), entry(5, 5, vec![0])])
            .is_err());
        assert_eq!(d.checkpoint(1000000).unwrap(), before);
        commit(&mut d, 4, first);
        let image = d.checkpoint(1000000).unwrap();
        for end in 0..image.len() {
            let mut dest = fresh(3);
            assert!(dest
                .restore_checkpoint(16, d.applied_index(), &image[..end])
                .is_err());
            assert_eq!(dest.applied_index(), 0);
        }
        assert_eq!(
            recover(&d)
                .group_creation_cancellation_at(4, p.creation.operation)
                .unwrap()
                .is_some(),
            cancel_first
        );
    }
}
