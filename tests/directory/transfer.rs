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
use voteboat::transfer::*;

pub(super) fn intent() -> TransferIntent {
    let before =
        ResponsibilityManifest::new(input(10, range(0, 256), ExecutionMode::Single(group(20))))
            .unwrap();
    let mut after = before.clone().into_input();
    after.epoch = OwnershipEpoch::new(2).unwrap();
    after.generation = RouteGeneration::new(2).unwrap();
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
    TransferIntent::new(before, ResponsibilityManifest::new(after).unwrap()).unwrap()
}
fn fresh() -> Directory {
    Directory::new(
        DirectoryPlan::new(group(1), vec![intent().before().clone()]).unwrap(),
        DirectoryLimits {
            operations: 20,
            history_bytes: 64 * 1024,
        },
    )
    .unwrap()
}
fn ready() -> Directory {
    let mut app = initialize(fresh());
    publish(&mut app, 1, 100, intent().before().clone(), None);
    app
}

#[test]
fn checked_split_and_merge_shape_bind_epochs_identity_and_distinct_groups() {
    let split = intent();
    assert_eq!(split.sources().len(), 1);
    assert_eq!(split.targets().len(), 2);
    let mut after = split.after().clone().into_input();
    after.execution = ExecutionMode::Single(group(23));
    after.epoch = OwnershipEpoch::new(3).unwrap();
    after.generation = RouteGeneration::new(3).unwrap();
    let merge = TransferIntent::new(
        split.after().clone(),
        ResponsibilityManifest::new(after).unwrap(),
    )
    .unwrap();
    assert_eq!(merge.sources().len(), 2);
    assert_eq!(merge.targets().len(), 1);
    for field in 0..9 {
        let mut after = split.after().clone().into_input();
        match field {
            0 => after.epoch = OwnershipEpoch::new(4).unwrap(),
            1 => after.generation = RouteGeneration::new(4).unwrap(),
            2 => after.application.version += 1,
            3 => after.scheme.version += 1,
            4 => after.authority = group(9),
            5 => after.state = ResponsibilityState::Fenced,
            6 => {
                let ExecutionMode::Partitioned(v) = &mut after.execution else {
                    unreachable!()
                };
                v[0].target = RouteTarget::Group(group(20));
            }
            7 => {
                let ExecutionMode::Partitioned(v) = &mut after.execution else {
                    unreachable!()
                };
                v[1].target = v[0].target;
            }
            _ => after.execution = ExecutionMode::Single(group(22)),
        }
        assert!(TransferIntent::new(
            split.before().clone(),
            ResponsibilityManifest::new(after).unwrap()
        )
        .is_err());
    }
    let mut before = split.before().clone().into_input();
    before.parent = Some(ParentAuthority {
        responsibility: id(90),
        group: group(90),
    });
    let mut after = split.after().clone().into_input();
    after.parent = before.parent;
    assert_eq!(
        TransferIntent::new(
            ResponsibilityManifest::new(before).unwrap(),
            ResponsibilityManifest::new(after).unwrap()
        )
        .unwrap_err()
        .0,
        RoutingError::WrongParent
    );
}

#[test]
fn directory_records_intent_without_publishing_ownership_and_locks_changes() {
    let intent = intent();
    let bytes = intent.encode(MAX_TRANSFER_INTENT_BYTES).unwrap();
    let mut app = ready();
    let receipt = app.apply_batch(&[entry(2, 200, bytes.clone())]).unwrap()[0];
    assert_eq!(receipt.outcome, DirectoryOutcome::TransferIntentRecorded);
    let status = app
        .transfer_intent_at(3, OperationId::new(200).unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(status.index, 3);
    assert_eq!(status.intent, intent);
    assert_eq!(app.manifest(id(10)), Some(intent.before()));
    let retry = app.apply_batch(&[entry(3, 200, bytes.clone())]).unwrap()[0];
    assert!(retry.duplicate);
    assert_eq!(
        app.transfer_intent_at(4, status.operation).unwrap(),
        Some(status.clone())
    );
    assert_eq!(
        app.apply_batch(&[entry(4, 201, bytes)]).unwrap()[0].outcome,
        DirectoryOutcome::LifecycleBusy
    );
    assert_eq!(
        publish(
            &mut app,
            5,
            202,
            update_manifest(intent.before(), 2),
            Some(1)
        )
        .outcome,
        DirectoryOutcome::LifecycleBusy
    );
    assert_eq!(
        publish(&mut app, 6, 203, intent.after().clone(), Some(1)).outcome,
        DirectoryOutcome::LifecycleBusy
    );
    assert_eq!(app.manifest(id(10)), Some(intent.before()));
    assert_eq!(
        app.transfer_intent_at(8, status.operation),
        Err(ApplicationError::NotApplied)
    );
}

#[test]
fn unknown_or_stale_intents_reject_and_retries_keep_original_failure() {
    let intent = intent();
    let bytes = intent.encode(MAX_TRANSFER_INTENT_BYTES).unwrap();
    let mut app = initialize(fresh());
    assert_eq!(
        app.apply_batch(&[entry(1, 200, bytes.clone())]).unwrap()[0].outcome,
        DirectoryOutcome::GenerationMismatch
    );
    publish(&mut app, 2, 100, intent.before().clone(), None);
    assert_eq!(
        app.apply_batch(&[entry(3, 200, bytes.clone())]).unwrap()[0].outcome,
        DirectoryOutcome::GenerationMismatch
    );
    assert_eq!(
        app.transfer_intent_at(0, OperationId::new(200).unwrap())
            .unwrap(),
        None
    );
    assert_eq!(
        app.apply_batch(&[entry(4, 201, bytes)]).unwrap()[0].outcome,
        DirectoryOutcome::TransferIntentRecorded
    );
    let other = DirectoryPlan::new(
        group(1),
        vec![ResponsibilityManifest::new(input(
            99,
            range(0, 256),
            ExecutionMode::Single(group(99)),
        ))
        .unwrap()],
    )
    .unwrap();
    let mut app = initialize(
        Directory::new(
            other,
            DirectoryLimits {
                operations: 20,
                history_bytes: 64 * 1024,
            },
        )
        .unwrap(),
    );
    assert_eq!(
        app.apply_batch(&[entry(
            1,
            200,
            intent.encode(MAX_TRANSFER_INTENT_BYTES).unwrap()
        )])
        .unwrap()[0]
            .outcome,
        DirectoryOutcome::UnknownResponsibility
    );
}

#[test]
fn intent_command_and_checkpoint_truncations_are_atomic_and_preserve_locks() {
    let intent = intent();
    let bytes = intent.encode(MAX_TRANSFER_INTENT_BYTES).unwrap();
    let mut app = ready();
    let before = app.checkpoint(100000).unwrap();
    for end in 0..bytes.len() {
        assert!(TransferIntent::decode(&bytes[..end]).is_err());
        assert!(app
            .apply_batch(&[entry(2, 200, bytes[..end].to_vec())])
            .is_err());
        assert_eq!(app.checkpoint(100000).unwrap(), before);
    }
    app.apply_batch(&[entry(2, 200, bytes)]).unwrap();
    let checkpoint = app.checkpoint(100000).unwrap();
    let mut recovered = fresh();
    let before = recovered.checkpoint(100000).unwrap();
    for end in 0..checkpoint.len() {
        assert!(recovered
            .restore_checkpoint(1, 3, &checkpoint[..end])
            .is_err());
        assert_eq!(recovered.checkpoint(100000).unwrap(), before);
    }
    recovered.restore_checkpoint(1, 3, &checkpoint).unwrap();
    assert_eq!(
        recovered
            .transfer_intent_at(3, OperationId::new(200).unwrap())
            .unwrap(),
        app.transfer_intent_at(3, OperationId::new(200).unwrap())
            .unwrap()
    );
    assert_eq!(
        publish(&mut recovered, 3, 201, intent.after().clone(), Some(1)).outcome,
        DirectoryOutcome::LifecycleBusy
    );
}

#[test]
fn lifecycle_read_view_preserves_original_api_and_counts_nested_result_capacity() {
    let intent = intent();
    let mut app = ready();
    app.apply_batch(&[entry(
        2,
        200,
        intent.encode(MAX_TRANSFER_INTENT_BYTES).unwrap(),
    )])
    .unwrap();
    let view = LifecycleDirectory::new(app);
    let query = DirectoryQuery::Transfer(OperationId::new(200).unwrap());
    let result = view.read_at(3, query).unwrap();
    let nested = view.read_result_bytes(&result, usize::MAX).unwrap();
    assert_eq!(
        view.read_result_bound(&query).unwrap(),
        std::mem::size_of::<DirectoryRead>() + nested
    );
    assert!(nested > 0);
    assert_eq!(
        view.read_result_bytes(&result, nested - 1),
        Err(ApplicationError::ReceiptBudget)
    );
    assert_eq!(view.read_at(4, query), Err(ApplicationError::NotApplied));
    assert_eq!(
        view.directory().read_at(3, id(10)).unwrap(),
        Some(intent.before().clone())
    );
    assert_eq!(
        view.read_at(3, DirectoryQuery::Transfer(OperationId::new(999).unwrap()))
            .unwrap(),
        DirectoryRead::Transfer(None)
    );
    let mut recovered = LifecycleDirectory::new(fresh());
    recovered
        .restore_checkpoint(1, 3, &view.checkpoint(100000).unwrap())
        .unwrap();
    assert_eq!(recovered.read_at(3, query).unwrap(), result);
}

#[test]
fn target_group_reservations_cover_other_assignments_and_overlapping_intents() {
    let a = intent();
    let mut before = a.before().clone().into_input();
    before.responsibility = id(11);
    before.execution = ExecutionMode::Single(group(30));
    let mut after = a.after().clone().into_input();
    after.responsibility = id(11);
    let ExecutionMode::Partitioned(routes) = &mut after.execution else {
        unreachable!()
    };
    routes[1].target = RouteTarget::Group(group(31));
    let b = TransferIntent::new(
        ResponsibilityManifest::new(before).unwrap(),
        ResponsibilityManifest::new(after).unwrap(),
    )
    .unwrap();
    let occupied =
        ResponsibilityManifest::new(input(12, range(0, 256), ExecutionMode::Single(group(50))))
            .unwrap();
    let plan = DirectoryPlan::new(
        group(1),
        vec![a.before().clone(), b.before().clone(), occupied.clone()],
    )
    .unwrap();
    let mut app = initialize(
        Directory::new(
            plan,
            DirectoryLimits {
                operations: 20,
                history_bytes: 64 * 1024,
            },
        )
        .unwrap(),
    );
    for (i, m) in [a.before().clone(), b.before().clone(), occupied]
        .into_iter()
        .enumerate()
    {
        publish(&mut app, i as u64 + 1, 100 + i as u128, m, None);
    }
    let mut after = a.after().clone().into_input();
    let ExecutionMode::Partitioned(routes) = &mut after.execution else {
        unreachable!()
    };
    routes[0].target = RouteTarget::Group(group(50));
    let busy = TransferIntent::new(
        a.before().clone(),
        ResponsibilityManifest::new(after).unwrap(),
    )
    .unwrap();
    let mut other = app.clone();
    assert_eq!(
        other
            .apply_batch(&[entry(
                4,
                200,
                busy.encode(MAX_TRANSFER_INTENT_BYTES).unwrap()
            )])
            .unwrap()[0]
            .outcome,
        DirectoryOutcome::TransferGroupBusy
    );
    assert_eq!(
        app.apply_batch(&[entry(4, 200, a.encode(MAX_TRANSFER_INTENT_BYTES).unwrap())])
            .unwrap()[0]
            .outcome,
        DirectoryOutcome::TransferIntentRecorded
    );
    let checkpoint = app.checkpoint(100000).unwrap();
    let mut recovered = Directory::new(app.plan().clone(), app.limits()).unwrap();
    recovered.restore_checkpoint(1, 5, &checkpoint).unwrap();
    assert_eq!(
        recovered
            .apply_batch(&[entry(5, 201, b.encode(MAX_TRANSFER_INTENT_BYTES).unwrap())])
            .unwrap()[0]
            .outcome,
        DirectoryOutcome::TransferGroupBusy
    );
    assert_eq!(recovered.manifest(id(11)), Some(b.before()));
    let mut after = b.after().clone().into_input();
    let ExecutionMode::Partitioned(routes) = &mut after.execution else {
        unreachable!()
    };
    routes[0].target = RouteTarget::Group(group(32));
    let distinct = TransferIntent::new(
        b.before().clone(),
        ResponsibilityManifest::new(after).unwrap(),
    )
    .unwrap();
    assert_eq!(
        recovered
            .apply_batch(&[entry(
                6,
                202,
                distinct.encode(MAX_TRANSFER_INTENT_BYTES).unwrap()
            )])
            .unwrap()[0]
            .outcome,
        DirectoryOutcome::TransferIntentRecorded
    );
}

#[test]
fn pending_intents_are_bounded_and_constructor_rejection_preserves_route_buffer() {
    let a = intent();
    let bytes = a.encode(MAX_TRANSFER_INTENT_BYTES).unwrap();
    let app = ready();
    assert_eq!(
        app.validate_proposal(
            OperationId::new(200).unwrap(),
            &bytes,
            std::iter::repeat_n(
                (OperationId::new(200).unwrap(), bytes.as_slice()),
                MAX_DIRECTORY_PENDING + 1
            )
        ),
        Err(ApplicationError::DedupCapacity)
    );
    let mut after = a.after().clone().into_input();
    let ExecutionMode::Partitioned(routes) = &mut after.execution else {
        unreachable!()
    };
    routes[1].target = routes[0].target;
    let pointer = routes.as_ptr();
    let (_, _, after) = TransferIntent::new(
        a.before().clone(),
        ResponsibilityManifest::new(after).unwrap(),
    )
    .unwrap_err();
    let ExecutionMode::Partitioned(routes) = &after.input().execution else {
        unreachable!()
    };
    assert_eq!(routes.as_ptr(), pointer);
}
