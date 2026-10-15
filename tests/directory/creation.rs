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
use voteboat::quorum::{Limits, Policy, Tree, WeightedChild};
pub(super) fn intent() -> GroupCreationIntent {
    let mut b = bootstrap(100, 3);
    b.policy = Policy::new(
        Tree::Majority(vec![
            Tree::Voter(node(1)),
            Tree::Weighted(vec![
                WeightedChild {
                    weight: 2,
                    node: Tree::Voter(node(2)),
                },
                WeightedChild {
                    weight: 1,
                    node: Tree::Voter(node(3)),
                },
            ]),
        ]),
        Limits::default(),
    )
    .unwrap();
    GroupCreationIntent {
        authority: group(1),
        parent: id(1),
        expected: RouteGeneration::new(1).unwrap(),
        responsibility: id(50),
        bootstrap: b,
        application: ApplicationAdapter {
            id: ApplicationAdapterId::new(7).unwrap(),
            version: 1,
        },
        mode: GroupCreationMode::Empty,
    }
}
fn creation_fresh() -> Directory {
    fresh_directory()
        .with_group_creation()
        .unwrap_or_else(|_| panic!("fresh creation mode"))
}
fn prepared() -> Directory {
    let mut app = initialize(creation_fresh());
    assert_eq!(
        publish(&mut app, 1, 1, manifests()[0].clone(), None).outcome,
        DirectoryOutcome::Published(RouteGeneration::new(1).unwrap())
    );
    app
}
fn apply(app: &mut Directory, op: u128, intent: &GroupCreationIntent) -> DirectoryReceipt {
    let bytes = intent.encode(MAX_DIRECTORY_COMMAND_BYTES).unwrap();
    app.validate_proposal(OperationId::new(op).unwrap(), &bytes, std::iter::empty())
        .unwrap();
    app.apply_batch(&[entry(app.applied_index(), op, bytes)])
        .unwrap()[0]
}
#[test]
fn creation_codec_bounds_policy_counts_bindings_and_every_truncation() {
    for mode in [GroupCreationMode::Empty, GroupCreationMode::Staging] {
        let mut value = intent();
        value.mode = mode;
        let bytes = value.encode(MAX_GROUP_CREATION_BYTES).unwrap();
        assert_eq!(value.encode(bytes.len()).unwrap(), bytes);
        assert!(value.encode(bytes.len() - 1).is_err());
        assert_eq!(GroupCreationIntent::decode(&bytes).unwrap(), value);
        for end in 0..bytes.len() {
            assert!(GroupCreationIntent::decode(&bytes[..end]).is_err(), "{end}");
        }
        for i in 0..bytes.len() {
            let mut changed = bytes.clone();
            changed[i] ^= 0xff;
            if let Ok(decoded) = GroupCreationIntent::decode(&changed) {
                assert_eq!(decoded.encode(MAX_GROUP_CREATION_BYTES).unwrap(), changed);
            }
        }
        let mut too_many = bytes.clone();
        too_many[142..146].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(GroupCreationIntent::decode(&too_many).is_err());
        let count_offset = bytes.len() - 4 - 32 * value.bootstrap.voter_stores.len();
        let mut stores = bytes.clone();
        stores[count_offset..count_offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(GroupCreationIntent::decode(&stores).is_err());
        let mut duplicate = bytes.clone();
        let first = duplicate[count_offset + 4..count_offset + 12].to_vec();
        duplicate[count_offset + 36..count_offset + 44].copy_from_slice(&first);
        assert!(GroupCreationIntent::decode(&duplicate).is_err());
        let mut trailing = bytes;
        trailing.push(0);
        assert!(GroupCreationIntent::decode(&trailing).is_err());
    }
    let mut bad = intent();
    bad.bootstrap.voter_stores.remove(&node(3));
    assert!(bad.encode(MAX_GROUP_CREATION_BYTES).is_err());
    let mut bad = intent();
    bad.application.version = 0;
    assert!(bad.encode(MAX_GROUP_CREATION_BYTES).is_err());
    assert!(GroupCreationIntent::decode(&vec![0; MAX_GROUP_CREATION_BYTES + 1]).is_err());
}
#[test]
fn creation_reserves_only_new_identity_and_never_publishes_ownership() {
    let mut app = prepared();
    let original = intent();
    let receipt = apply(&mut app, 10, &original);
    assert_eq!(receipt.outcome, DirectoryOutcome::CreationReserved);
    assert!(!receipt.duplicate);
    let status = app
        .group_creation_at(app.applied_index(), group(100))
        .unwrap()
        .unwrap();
    assert_eq!(status.index, receipt.index);
    assert_eq!(status.operation, OperationId::new(10).unwrap());
    assert_eq!(status.intent, original);
    assert!(app
        .group_creation_at(app.applied_index() + 1, group(100))
        .is_err());
    assert!(app.manifest(id(50)).is_none());
    let retry = apply(&mut app, 10, &original);
    assert!(retry.duplicate);
    assert_eq!(retry.outcome, receipt.outcome);
    assert_eq!(
        app.group_creation_at(app.applied_index(), group(100))
            .unwrap()
            .unwrap(),
        status
    );
    let mut conflict = original.clone();
    conflict.mode = GroupCreationMode::Staging;
    assert_eq!(
        apply(&mut app, 10, &conflict).outcome,
        DirectoryOutcome::OperationConflict
    );
    assert_eq!(
        apply(&mut app, 11, &original).outcome,
        DirectoryOutcome::CreationConflict
    );
    let mut incarnation = original.clone();
    incarnation.bootstrap.group.incarnation = GroupIncarnation::new(2).unwrap();
    incarnation.responsibility = id(51);
    assert_eq!(
        apply(&mut app, 12, &incarnation).outcome,
        DirectoryOutcome::CreationConflict
    );
    let mut child = original;
    child.bootstrap.group = group(101);
    assert_eq!(
        apply(&mut app, 13, &child).outcome,
        DirectoryOutcome::CreationConflict
    );
    assert_eq!(app.manifest(id(1)), Some(&manifests()[0]));
}
#[test]
fn creation_parent_authority_generation_and_known_routes_refuse() {
    let original = intent();
    let mut cases = Vec::new();
    let mut v = original.clone();
    v.authority = group(2);
    cases.push((v, DirectoryOutcome::OwnershipChange));
    let mut v = original.clone();
    v.parent = id(99);
    cases.push((v, DirectoryOutcome::UnknownResponsibility));
    let mut v = original.clone();
    v.expected = RouteGeneration::new(2).unwrap();
    cases.push((v, DirectoryOutcome::GenerationMismatch));
    let mut v = original.clone();
    v.bootstrap.group = group(20);
    cases.push((v, DirectoryOutcome::CreationConflict));
    let mut v = original;
    v.responsibility = id(3);
    cases.push((v, DirectoryOutcome::CreationConflict));
    for (value, outcome) in cases {
        let mut app = prepared();
        assert_eq!(apply(&mut app, 10, &value).outcome, outcome);
        assert!(app
            .group_creation_at(app.applied_index(), value.bootstrap.group)
            .unwrap()
            .is_none());
        assert!(app.manifest(id(50)).is_none());
    }
}
#[test]
fn creation_checkpoint_replays_reservations_and_atomic_batch_rejects_partial_progress() {
    let mut app = prepared();
    let value = intent();
    let bytes = value.encode(MAX_GROUP_CREATION_BYTES).unwrap();
    let before = app.checkpoint(1_000_000).unwrap();
    let entries = [
        entry(app.applied_index(), 10, bytes.clone()),
        entry(app.applied_index() + 1, 11, vec![1]),
    ];
    assert!(app.apply_batch(&entries).is_err());
    assert_eq!(app.checkpoint(1_000_000).unwrap(), before);
    apply(&mut app, 10, &value);
    let status = app
        .group_creation_at(app.applied_index(), group(100))
        .unwrap();
    let checkpoint = app.checkpoint(1_000_000).unwrap();
    let mut restored = creation_fresh();
    restored
        .restore_checkpoint(
            CREATION_DIRECTORY_APPLICATION_SCHEMA,
            app.applied_index(),
            &checkpoint,
        )
        .unwrap();
    assert_eq!(
        restored
            .group_creation_at(app.applied_index(), group(100))
            .unwrap(),
        status
    );
    assert_eq!(
        apply(&mut restored, 10, &value).outcome,
        DirectoryOutcome::CreationReserved
    );
    assert_eq!(
        apply(&mut restored, 11, &value).outcome,
        DirectoryOutcome::CreationConflict
    );
    for end in 0..checkpoint.len() {
        let mut truncated = creation_fresh();
        assert!(truncated
            .restore_checkpoint(
                CREATION_DIRECTORY_APPLICATION_SCHEMA,
                app.applied_index(),
                &checkpoint[..end]
            )
            .is_err());
        assert!(!truncated.is_initialized());
    }
}
#[test]
fn creation_admission_reserves_original_history_capacity() {
    let mut app = prepared();
    let value = intent();
    let bytes = value.encode(MAX_GROUP_CREATION_BYTES).unwrap();
    let mut pending = Vec::new();
    for operation in 10..10 + app.remaining_operations() as u128 {
        pending.push((OperationId::new(operation).unwrap(), bytes.as_slice()));
    }
    assert_eq!(
        app.validate_proposal(
            OperationId::new(99).unwrap(),
            &bytes,
            pending.iter().copied()
        ),
        Err(ApplicationError::DedupCapacity)
    );
    assert!(app
        .group_creation_at(app.applied_index(), group(100))
        .unwrap()
        .is_none());
    apply(&mut app, 10, &value);
    assert!(app
        .validate_proposal(OperationId::new(10).unwrap(), &bytes, std::iter::empty())
        .is_ok());
}

#[cfg(feature = "native")]
#[test]
fn creation_native_wal_every_torn_frame_and_barrier_failure_recovers_old_or_complete_intent() {
    use voteboat::native::log_store::*;
    let limits = LogLimits::default();
    let prefix = vec![
        entry(
            0,
            1_000_000,
            creation_fresh()
                .bootstrap_command(MAX_DIRECTORY_COMMAND_BYTES)
                .unwrap(),
        ),
        entry(1, 1, command(manifests()[0].clone(), None)),
    ];
    let seed = || {
        let io = ModelIo::default();
        let mut log = NativeLogStore::create(io.clone(), identity(1), limits).unwrap();
        append(&mut log, vec![LogMutation::Create(bootstrap(1, 3))]);
        let state = log.state(group(1)).unwrap();
        append(
            &mut log,
            vec![update(
                &state,
                1,
                2,
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
    let value = intent();
    let mutation = update(
        &state,
        1,
        3,
        Some(Suffix {
            from: 3,
            entries: vec![entry(
                2,
                10,
                value.encode(MAX_GROUP_CREATION_BYTES).unwrap(),
            )],
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
        let recovered = NativeLogStore::recover(io, identity(1), limits).unwrap();
        let state = recovered.state(group(1)).unwrap();
        assert!(state.commit_index == 2 || state.commit_index == 3);
        let mut app = creation_fresh();
        let committed: Vec<_> = state
            .entries
            .into_iter()
            .filter(|e| e.index <= state.commit_index)
            .collect();
        app.apply_batch(&committed).unwrap();
        let status = app
            .group_creation_at(state.commit_index, group(100))
            .unwrap();
        if state.commit_index == 2 {
            old = true;
            assert!(status.is_none());
        } else {
            complete = true;
            assert_eq!(status.unwrap().intent, value);
        }
        assert!(app.manifest(id(50)).is_none());
    }
    assert!(
        old && complete,
        "fault model must cover both publication outcomes"
    );
}

#[test]
fn creation_mode_is_bound_before_apply_and_cross_schema_recovery_fails_closed() {
    let legacy = fresh_directory();
    let creation = creation_fresh();
    assert_eq!(legacy.schema_version(), DIRECTORY_APPLICATION_SCHEMA);
    assert_eq!(
        creation.schema_version(),
        CREATION_DIRECTORY_APPLICATION_SCHEMA
    );
    assert_eq!(
        legacy.group_creation_at(0, group(100)),
        Err(ApplicationError::UnsupportedSchema)
    );
    assert_ne!(
        legacy
            .bootstrap_command(MAX_DIRECTORY_COMMAND_BYTES)
            .unwrap(),
        creation
            .bootstrap_command(MAX_DIRECTORY_COMMAND_BYTES)
            .unwrap()
    );
    for (mut destination, source) in [(legacy, creation), (creation_fresh(), fresh_directory())] {
        let source = initialize(source);
        let bytes = source.checkpoint(1_000_000).unwrap();
        assert!(destination
            .restore_checkpoint(source.schema_version(), source.applied_index(), &bytes)
            .is_err());
        assert!(destination
            .restore_checkpoint(destination.schema_version(), source.applied_index(), &bytes)
            .is_err());
        assert!(!destination.is_initialized());
        assert!(destination
            .apply_batch(&[entry(
                0,
                99,
                source
                    .bootstrap_command(MAX_DIRECTORY_COMMAND_BYTES)
                    .unwrap()
            )])
            .is_err());
        assert_eq!(destination.applied_index(), 0);
    }
    let mut legacy = initialize(fresh_directory());
    assert!(legacy
        .apply_batch(&[entry(
            1,
            10,
            intent().encode(MAX_GROUP_CREATION_BYTES).unwrap()
        )])
        .is_err());
    assert!(legacy.with_group_creation().is_err());
}

#[test]
fn creation_does_not_bypass_parent_transfer_reservation() {
    let transfer = super::transfer::intent();
    let mut app = Directory::new(
        DirectoryPlan::new(group(1), vec![transfer.before().clone()]).unwrap(),
        DirectoryLimits {
            operations: 20,
            history_bytes: 64 * 1024,
        },
    )
    .unwrap()
    .with_group_creation()
    .unwrap_or_else(|_| panic!("fresh mode"));
    app = initialize(app);
    publish(&mut app, 1, 1, transfer.before().clone(), None);
    let receipt = app
        .apply_batch(&[entry(
            2,
            20,
            transfer
                .encode(voteboat::transfer::MAX_TRANSFER_INTENT_BYTES)
                .unwrap(),
        )])
        .unwrap()[0];
    assert_eq!(receipt.outcome, DirectoryOutcome::TransferIntentRecorded);
    let mut value = intent();
    value.parent = transfer.before().input().responsibility;
    assert_eq!(
        apply(&mut app, 10, &value).outcome,
        DirectoryOutcome::LifecycleBusy
    );
    assert!(app
        .group_creation_at(app.applied_index(), group(100))
        .unwrap()
        .is_none());
}
