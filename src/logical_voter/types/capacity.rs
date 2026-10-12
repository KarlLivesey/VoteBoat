// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! A necessary construction bound preserves the original replay/dedup contract.
use super::*;
use crate::{
    application::{CheckpointStateMachine, ProposalAdmission, StateMachine},
    log_format::NativeLogCodec,
    logical_voter::{Command, CommandBody, LogicalApplication},
    quorum::{Limits, Policy, Tree},
    snapshot_format::NativeSnapshotCodec,
    wire_format::NativeWireCodec,
};
use std::collections::BTreeMap;

fn construct(limits: LogicalLimits) -> Result<LogicalApplication, ApplicationError> {
    let group = |id| GroupIdentity {
        id: GroupId::new(id).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    };
    let store = |id| StoreIdentity {
        id: StoreId::new(id).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    };
    let bootstrap = |id: u64| {
        let node = NodeId::new(id).unwrap();
        Bootstrap {
            group: group(id.into()),
            configuration: ConfigurationId::new(1).unwrap(),
            policy: Policy::new(Tree::Voter(node), Limits::default()).unwrap(),
            voter_stores: BTreeMap::from([(node, store(id.into()))]),
        }
    };
    LogicalApplication::new(
        LogicalStoreMapping::new(group(1), NodeId::new(1).unwrap(), store(1), group(2))?,
        bootstrap(2),
        bootstrap(1),
        limits,
        NativeLogCodec,
        NativeSnapshotCodec,
        NativeWireCodec::with_membership(Default::default()).unwrap(),
    )
}

#[test]
fn retained_512_kib_cannot_be_declared_inside_a_48_kib_checkpoint() {
    let limits = LogicalLimits {
        parent_log: LogLimits {
            max_groups: 1,
            max_entries_per_group: 16,
            max_command_bytes: 1024,
            max_batch_bytes: 8192,
            max_batch_units: 1,
            max_pending_units: 8,
            max_snapshot_bytes: 64 * 1024,
            ..LogLimits::default()
        },
        parent_snapshots: SnapshotLimits {
            max_application_bytes: 4096,
            max_metadata_bytes: 4096,
            max_chunk_bytes: 1024,
        },
        max_command_bytes: 32 * 1024,
        max_actions: 256,
        max_retained_action_bytes: 512 * 1024,
        control_actions: 16,
        control_action_bytes: 64 * 1024,
        max_capsule_bytes: 8192,
        max_parent_checkpoint_bytes: 32 * 1024,
        max_child_checkpoint_bytes: 48 * 1024,
    };
    assert!(matches!(
        limits.validate(),
        Err(ApplicationError::InvalidCommand)
    ));
    assert!(matches!(
        construct(limits),
        Err(ApplicationError::InvalidCommand)
    ));
    assert!(construct(LogicalLimits {
        max_child_checkpoint_bytes: 512 * 1024,
        ..limits
    })
    .is_ok());
    assert!(LogicalLimits {
        max_retained_action_bytes: usize::MAX,
        ..limits
    }
    .validate()
    .is_err());
}

#[test]
fn coherent_checkpoint_restores_the_original_command_and_duplicate_receipt() {
    let limits = LogicalLimits::default();
    let mut app = construct(limits).unwrap();
    let child = app.child_bootstrap();
    let (&node, &store) = child.voter_stores.first_key_value().unwrap();
    let command = Command {
        operation: OperationId::new(1).unwrap(),
        origin_configuration: child.configuration,
        representative: RepresentativeBinding {
            epoch: RepresentativeEpoch::new(1).unwrap(),
            physical: PeerIdentity { node, store },
            physical_session: StoreSession::new(1).unwrap(),
            logical_session: StoreSession::new(1).unwrap(),
        },
        body: CommandBody::Claim {
            expected_epoch: None,
        },
    };
    let bytes = app
        .encode_command(&command, limits.max_command_bytes)
        .unwrap();
    let entry = |index| LogEntry {
        index,
        term: 1,
        payload: EntryPayload::Command {
            operation: command.operation,
            bytes: bytes.clone(),
        },
    };
    let first = app.apply_batch(&[entry(1)]).unwrap().remove(0);
    assert_eq!(first.outcome, LogicalOutcome::Claimed);
    assert!(!first.duplicate);
    let checkpoint = app.checkpoint(limits.max_child_checkpoint_bytes).unwrap();
    let mut restored = construct(limits).unwrap();
    restored.restore_checkpoint(1, 1, &checkpoint).unwrap();
    assert_eq!(restored.checkpoint(usize::MAX).unwrap(), checkpoint);
    assert!(restored
        .validate_proposal(command.operation, &bytes, std::iter::empty())
        .is_ok());
    let duplicate = restored.apply_batch(&[entry(2)]).unwrap().remove(0);
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.first_index, first.first_index);
    assert_eq!(duplicate.outcome, first.outcome);
    let before = restored.checkpoint(usize::MAX).unwrap();
    let changed = restored
        .encode_command(
            &Command {
                body: CommandBody::Claim {
                    expected_epoch: Some(command.representative.epoch),
                },
                ..command
            },
            limits.max_command_bytes,
        )
        .unwrap();
    assert_eq!(
        restored.validate_proposal(OperationId::new(1).unwrap(), &changed, std::iter::empty()),
        Err(ApplicationError::OperationConflict)
    );
    assert_eq!(restored.checkpoint(usize::MAX).unwrap(), before);
}

#[test]
fn checkpoint_exports_exact_encoded_ownership_without_changing_bytes_or_errors() {
    let app = construct(LogicalLimits::default()).unwrap();
    let original = app.encode_state(usize::MAX).unwrap();
    let bound = original.len();
    let checkpoint = app.checkpoint(bound).unwrap();
    assert_eq!(checkpoint, original);
    assert_eq!(checkpoint.capacity(), bound);
    assert_eq!(
        app.checkpoint(bound - 1),
        Err(ApplicationError::InvalidCheckpoint)
    );
    let mut restored = construct(LogicalLimits::default()).unwrap();
    restored
        .restore_checkpoint(app.schema_version(), app.applied_index(), &checkpoint)
        .unwrap();
    assert_eq!(restored.checkpoint(bound).unwrap(), checkpoint);
    assert_eq!(app.encode_state(usize::MAX).unwrap(), original);
}
