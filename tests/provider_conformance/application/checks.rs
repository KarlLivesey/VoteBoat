// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
fn image<A: CheckpointStateMachine>(app: &A) -> Vec<u8> {
    app.checkpoint(4096).unwrap()
}
fn batch<S: Scenario>(app: &mut S::App, entries: &[LogEntry], expected: Vec<S::Reply>) {
    let before = image(app);
    let bound = app.receipt_bytes_bound(entries).unwrap();
    assert_eq!(
        image(app),
        before,
        "receipt bound changed application state"
    );
    let receipts = app.apply_batch(entries).unwrap();
    for receipt in &receipts {
        let nested = receipt.nested_bytes(bound).unwrap();
        if nested > 0 {
            assert!(
                receipt.nested_bytes(nested - 1).is_err(),
                "nested receipt limit was ignored"
            );
        }
    }
    let retained = receipts.capacity() * std::mem::size_of::<S::Receipt>()
        + receipts
            .iter()
            .map(|r| r.nested_bytes(bound).unwrap())
            .sum::<usize>();
    assert!(
        retained <= bound,
        "actual receipt capacity exceeds declared bound"
    );
    let identities = entries
        .iter()
        .filter_map(|entry| match entry.payload {
            EntryPayload::Command { operation, .. } => Some((entry.index, operation)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        receipts
            .iter()
            .map(|r| (r.index(), r.operation()))
            .collect::<Vec<_>>(),
        identities,
        "receipt identity/order/count differs from original commands"
    );
    assert_eq!(
        receipts.iter().map(S::reply).collect::<Vec<_>>(),
        expected,
        "application-specific original outcomes changed"
    );
    assert_eq!(
        app.applied_index(),
        entries.last().unwrap().index,
        "application did not advance through the contiguous batch"
    );
}
fn read<S: Scenario>(app: &S::App, replay: bool) {
    let before = image(app);
    let applied = app.applied_index();
    for required in [0, applied] {
        assert_eq!(
            S::value(app.read_at(required, S::query()).unwrap()),
            S::expected_value(replay)
        );
    }
    assert!(
        matches!(
            app.read_at(applied + 1, S::query()),
            Err(ApplicationError::NotApplied)
        ),
        "unapplied read boundary was accepted"
    );
    assert_eq!(image(app), before, "read changed application state");
}
fn rejected_batch<S: Scenario>(app: &mut S::App, entries: &[LogEntry], error: ApplicationError) {
    let before = image(app);
    let applied = app.applied_index();
    assert_eq!(app.apply_batch(entries).err(), Some(error));
    assert_eq!(
        app.applied_index(),
        applied,
        "failed batch changed applied boundary"
    );
    assert_eq!(image(app), before, "failed batch changed application state");
}
fn invalid_batches<S: Scenario>(app: &mut S::App) {
    let malformed = LogEntry {
        payload: EntryPayload::Command {
            operation: OperationId::new(4).unwrap(),
            bytes: vec![0],
        },
        ..S::command(5, 4, 1)
    };
    rejected_batch::<S>(
        app,
        &[S::command(4, 3, 5), malformed],
        ApplicationError::InvalidCommand,
    );
    rejected_batch::<S>(app, &[S::command(5, 3, 5)], ApplicationError::IndexGap);
    rejected_batch::<S>(
        app,
        &[S::command(4, 3, 5), S::command(5, 4, 6)],
        ApplicationError::DedupCapacity,
    );
}
fn restore_refusal<S: Scenario>(target: &mut S::App, schema: u64, index: u64, bytes: &[u8]) {
    let before = image(target);
    let applied = target.applied_index();
    assert!(target.restore_checkpoint(schema, index, bytes).is_err());
    assert_eq!(target.applied_index(), applied);
    assert_eq!(
        image(target),
        before,
        "failed restore changed application state"
    );
}
fn invalid_images<S: Scenario>(original: &S::App, bytes: &[u8]) {
    let mut target = S::fresh(3);
    target.apply_batch(&[S::command(1, 9, 99)]).unwrap();
    let schema = original.schema_version();
    let applied = original.applied_index();
    restore_refusal::<S>(&mut target, schema + 1, applied, bytes);
    restore_refusal::<S>(&mut target, schema, applied + 1, bytes);
    for end in 0..bytes.len() {
        restore_refusal::<S>(&mut target, schema, applied, &bytes[..end]);
    }
    let mut trailing = bytes.to_vec();
    trailing.push(0);
    restore_refusal::<S>(&mut target, schema, applied, &trailing);
    restore_refusal::<S>(&mut S::fresh(4), schema, applied, bytes);
    assert!(original.checkpoint(bytes.len() - 1).is_err());
}
fn cold_replay<S: Scenario>(original: &S::App, bytes: &[u8]) {
    let mut restored = S::fresh(3);
    restored
        .restore_checkpoint(original.schema_version(), original.applied_index(), bytes)
        .unwrap();
    assert_eq!(restored.applied_index(), original.applied_index());
    read::<S>(&restored, false);
    let entries = [
        S::command(4, 1, 7),
        S::command(5, 2, 11),
        S::command(6, 1, 8),
        S::command(7, 3, 5),
    ];
    batch::<S>(&mut restored, &entries, S::replies(true));
    read::<S>(&restored, true);
    rejected_batch::<S>(
        &mut restored,
        &[S::command(8, 4, 1)],
        ApplicationError::DedupCapacity,
    );
    let receipts = restored.apply_batch(&[S::command(8, 1, 7)]).unwrap();
    assert_eq!(receipts.len(), 1);
    assert_eq!(
        (receipts[0].index(), receipts[0].operation()),
        (8, OperationId::new(1).unwrap())
    );
    assert_eq!(S::reply(&receipts[0]), S::replies(true).remove(0));
    assert_eq!(restored.applied_index(), 8);
    read::<S>(&restored, true);
}
fn deployment<S: Scenario>(app: &S::App) {
    use voteboat::{
        identity::{GroupId, GroupIdentity, GroupIncarnation},
        raft::ReadinessRequirements,
    };
    let before = image(app);
    for id in [1, 2] {
        app.validate_group(GroupIdentity {
            id: GroupId::new(id).unwrap(),
            incarnation: GroupIncarnation::new(1).unwrap(),
        })
        .unwrap();
    }
    assert_eq!(app.deployment_requirements(), S::requirements());
    if let Some(required) = S::requirements() {
        app.validate_deployment_requirements(required).unwrap();
        app.validate_deployment_requirements(ReadinessRequirements {
            command_bytes: required.command_bytes + 1,
            snapshot_bytes: required.snapshot_bytes + 1,
            ..required
        })
        .unwrap();
        for invalid in [
            ReadinessRequirements {
                application_schema: required.application_schema + 1,
                ..required
            },
            ReadinessRequirements {
                command_bytes: required.command_bytes - 1,
                ..required
            },
            ReadinessRequirements {
                snapshot_bytes: required.snapshot_bytes - 1,
                ..required
            },
        ] {
            assert!(app.validate_deployment_requirements(invalid).is_err());
        }
    } else {
        assert_eq!(
            app.validate_deployment_requirements(ReadinessRequirements {
                application_schema: 1,
                command_bytes: 8,
                snapshot_bytes: 4096,
            }),
            Err(ApplicationError::InvalidCheckpoint)
        );
    }
    assert_eq!(
        image(app),
        before,
        "deployment validation changed application state"
    );
}
pub(super) fn exercise<S: Scenario>() {
    let mut app = S::fresh(3);
    assert_eq!(app.applied_index(), 0);
    deployment::<S>(&app);
    let first = [
        S::command(1, 1, 7),
        LogEntry {
            index: 2,
            term: 1,
            payload: EntryPayload::Noop,
        },
        S::command(3, 2, 11),
    ];
    batch::<S>(&mut app, &first, S::replies(false));
    read::<S>(&app, false);
    invalid_batches::<S>(&mut app);
    let bytes = image(&app);
    let mut independent = app.clone();
    independent.apply_batch(&[S::command(4, 3, 5)]).unwrap();
    assert_eq!(
        image(&app),
        bytes,
        "checkpoint clone aliased mutable application state"
    );
    invalid_images::<S>(&app, &bytes);
    cold_replay::<S>(&app, &bytes);
    assert_eq!(
        image(&app),
        bytes,
        "fresh restore/replay changed original application"
    );
}
